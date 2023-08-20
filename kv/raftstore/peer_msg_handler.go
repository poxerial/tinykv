package raftstore

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// ===============================Before Split===============================
// |StartKey|<----------------------Old Region---------------------->|EndKey|
// ===============================After Split================================
// |StartKey|<-----Old Region----->|Split Key|<------New Region----->|EndKey|
func (d *peerMsgHandler) applySplit(req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) []*metapb.Region {
	if !keyInRegion(req.Split.SplitKey, d.Region()) {
		return nil
	}
	var err error

	split := req.Split

	if d.IsLeader() {
		log.Debugf("%v leader starts to split.", d.Tag)
	}

	sort.Slice(split.NewPeerIds, func(i, j int) bool {
		return split.NewPeerIds[i] <= split.NewPeerIds[j]
	})

	sort.Slice(d.Region().Peers, func(i, j int) bool {
		return d.Region().Peers[i].Id <= d.Region().Peers[j].Id
	})

	peers := make([]*metapb.Peer, len(split.NewPeerIds))
	for i, id := range split.NewPeerIds {
		peers[i] = &metapb.Peer{
			Id:      id,
			StoreId: d.Region().Peers[i].StoreId,
		}
	}

	region := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: split.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: d.Region().RegionEpoch.ConfVer,
			Version: d.Region().RegionEpoch.Version + 1,
		},
		Peers: peers,
	}

	d.Region().EndKey = split.SplitKey
	d.Region().RegionEpoch.Version++

	go func() {
		region_state := &rspb.RegionLocalState{
			State:  rspb.PeerState_Normal,
			Region: region,
		}
		new_peer_kvWB := &engine_util.WriteBatch{}
		new_peer_kvWB.SetMeta(meta.RegionStateKey(region.Id), region_state)
		new_peer_kvWB.WriteToDB(d.ctx.engine.Kv)

		peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, region)
		if err != nil {
			panic(err.Error())
		}

		d.ctx.router.register(peer)
		_ = d.ctx.router.send(peer.regionId, message.Msg{Type: message.MsgTypeStart})

		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regions[region.Id] = region
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{
			region: region,
		})
		d.ctx.storeMeta.Unlock()
	}()

	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{
		region: d.Region(),
	})
	d.ctx.storeMeta.Unlock()

	err = kvWB.SetMeta(meta.RegionStateKey(d.regionId), &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: d.peerStorage.region,
	})
	if err != nil {
		panic(err.Error())
	}

	return []*metapb.Region{d.Region(), region}
}

func (d *peerMsgHandler) applyAdminRequestWithResp(req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) *raft_cmdpb.AdminResponse {
	resp := &raft_cmdpb.AdminResponse{
		CmdType: req.CmdType,
	}
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		// pass
	case raft_cmdpb.AdminCmdType_ChangePeer:
		panic("ChangePeer adminCmd shouldn't be proposed!")
	case raft_cmdpb.AdminCmdType_CompactLog:
		cg := req.CompactLog
		apply := d.peerStorage.applyState
		apply.TruncatedState = &rspb.RaftTruncatedState{
			Index: cg.CompactIndex,
			Term:  cg.CompactTerm,
		}

		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), apply)
		if err != nil {
			panic(err.Error())
		}

		d.ScheduleCompactLog(cg.CompactIndex)

		resp.CompactLog = &raft_cmdpb.CompactLogResponse{}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
		resp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
	case raft_cmdpb.AdminCmdType_Split:
		regions := d.applySplit(req, kvWB)
		resp.Split = &raft_cmdpb.SplitResponse{
			Regions: regions,
		}
	}
	return resp
}

func (d *peerMsgHandler) applyAdminRequest(req *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		// pass
	case raft_cmdpb.AdminCmdType_ChangePeer:
		panic("ChangePeer adminCmd shouldn't be proposed!")
	case raft_cmdpb.AdminCmdType_CompactLog:
		cg := req.CompactLog
		apply := d.peerStorage.applyState
		apply.TruncatedState = &rspb.RaftTruncatedState{
			Index: cg.CompactIndex,
			Term:  cg.CompactTerm,
		}

		err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), apply)
		if err != nil {
			panic(err.Error())
		}

		d.ScheduleCompactLog(cg.CompactIndex)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
	case raft_cmdpb.AdminCmdType_Split:
		d.applySplit(req, kvWB)
	}
}

func (d *peerMsgHandler) applyConfChange(entry *eraftpb.Entry) *metapb.Region {
	cc := eraftpb.ConfChange{}
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		panic(err.Error())
	}

	ctx := eraftpb.ConfChangeContext{}
	err = ctx.Unmarshal(cc.Context)
	if err != nil {
		panic(err.Error())
	}

	if ctx.Confver != d.Region().RegionEpoch.ConfVer {
		return d.Region()
	}

	region := d.Region()

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
		for _, p := range region.Peers {
			if p.Id == cc.NodeId {
				log.Debugf("Can't add duplicated peer %v in group %v", cc.NodeId, region.Peers)
				return region
			}
		}
		peer := &metapb.Peer{
			Id:      cc.NodeId,
			StoreId: ctx.StoreId,
		}
		region.Peers = append(region.Peers, peer)
		d.peerCache[peer.Id] = peer
	} else if cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		remove_index := -1
		for i, peer := range region.Peers {
			if peer.Id == cc.NodeId {
				remove_index = i
			}
		}
		if remove_index == -1 {
			log.Debugf("Can't remove peer %v, no such peer in group %v", cc.NodeId, region.Peers)
			return region
		}
		if remove_index < len(region.Peers)-1 {
			region.Peers = append(region.Peers[:remove_index], region.Peers[remove_index+1:]...)
		} else {
			region.Peers = region.Peers[:remove_index]
		}
	}

	region.RegionEpoch.ConfVer++

	store_meta := d.ctx.storeMeta
	store_meta.Lock()
	store_meta.regions[region.Id] = region
	item := regionItem{
		region: region,
	}
	store_meta.regionRanges.ReplaceOrInsert(&item)
	store_meta.Unlock()

	ps := rspb.PeerState_Normal
	if d.PeerId() == cc.NodeId &&
		d.storeID() == ctx.StoreId &&
		cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		ps = rspb.PeerState_Tombstone
	}
	st := &rspb.RegionLocalState{
		State:  ps,
		Region: region,
	}

	kvWB := &engine_util.WriteBatch{}
	err = kvWB.SetMeta(meta.RegionStateKey(region.Id), st)
	if err != nil {
		panic(err.Error())
	}
	err = kvWB.WriteToDB(d.ctx.engine.Kv)
	if err != nil {
		panic(err.Error())
	}

	if ctx.StoreId == d.storeID() &&
		cc.NodeId == d.PeerId() &&
		cc.ChangeType == eraftpb.ConfChangeType_RemoveNode {
		d.destroyPeer()
	}

	d.RaftGroup.ApplyConfChange(cc)
	return region
}

func (d *peerMsgHandler) apply(entry *eraftpb.Entry) bool {
	if entry.Data == nil || len(entry.Data) == 0 {
		return false
	}

	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		region := d.applyConfChange(entry)
		destroy := true
		for _, p := range region.Peers {
			if p.Id == d.PeerId() {
				destroy = false
			}
		}
		return destroy
	}

	kvWB := &engine_util.WriteBatch{}
	cmd := &raft_cmdpb.RaftCmdRequest{}
	err := cmd.Unmarshal(entry.Data)
	if err != nil {
		panic(err.Error())
	}
	for _, request := range cmd.Requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Invalid:
			panic("Invalid CmdType")
		case raft_cmdpb.CmdType_Get:
		case raft_cmdpb.CmdType_Put:
			if !keyInRegion(request.Put.Key, d.Region()) {
				continue
			}
			kvWB.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			if !keyInRegion(request.Delete.Key, d.Region()) {
				continue
			}
			kvWB.DeleteCF(request.Delete.Cf, request.Delete.Key)
		case raft_cmdpb.CmdType_Snap:
		}
	}

	admin_req := cmd.AdminRequest
	if admin_req != nil {
		d.applyAdminRequest(admin_req, kvWB)
	}

	err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		panic(err.Error())
	}
	kvWB.Reset()
	return false
}

func (d *peerMsgHandler) applyWithResp(entry *eraftpb.Entry) bool {
	if len(entry.Data) == 0 {
		return false
	}

	resp := newCmdResp()
	BindRespTerm(resp, d.peerStorage.raftState.HardState.Term)

	var curr *proposal = nil
	curr_i := 0

	for i, p := range d.proposals {
		if p.index == entry.Index && p.term == entry.Term {
			curr = p
			curr_i = i
		}
	}
	if curr == nil {
		log.Info("d.proposals: [")
		for _, p := range d.proposals {
			log.Infof("%v, ", *p)
		}
		log.Info("]")
		log.Fatalf("Can't find proposal!log term: %v index: %v, peer term: %v", entry.Term, entry.Index, d.Term())
	}

	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		region := d.applyConfChange(entry)

		if curr_i+1 < len(d.proposals) {
			d.proposals = append(d.proposals[0:curr_i], d.proposals[curr_i+1:]...)
		} else {
			d.proposals = d.proposals[0:curr_i]
		}

		destroy := true
		for _, p := range region.Peers {
			if p.Id == d.PeerId() {
				destroy = false
			}
		}
		if destroy {
			return true
		}

		curr.cb.Done(resp)

		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: region,
			},
		}

		return false
	}

	cmd := &raft_cmdpb.RaftCmdRequest{}
	err := cmd.Unmarshal(entry.Data)
	if err != nil {
		panic(err.Error())
	}

	kvWB := &engine_util.WriteBatch{}

	responses := make([]*raft_cmdpb.Response, len(cmd.Requests))
	for i, request := range cmd.Requests {
		response := &raft_cmdpb.Response{CmdType: request.CmdType}
		switch request.CmdType {
		case raft_cmdpb.CmdType_Invalid:
		case raft_cmdpb.CmdType_Get:
			if !keyInRegion(request.Get.Key, d.Region()) {
				curr.cb.Done(ErrResp(
					&util.ErrKeyNotInRegion{
						Key:    request.Get.Key,
						Region: d.Region(),
					},
				))
				return false
			}
			if len(cmd.Requests) != 1 {
				log.Fatal("Can't handle multiple get requests in one RaftCmd!")
			}
			var value []byte
			err = d.peerStorage.Engines.Kv.View(func(txn *badger.Txn) error {
				item, err := txn.Get(engine_util.KeyWithCF(request.Get.Cf, request.Get.Key))
				if err != nil {
					if err == badger.ErrKeyNotFound {
						value = make([]byte, 0)
						return nil
					}
					return err
				}
				value, err = item.Value()
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				panic(err.Error())
			}
			response.Get = &raft_cmdpb.GetResponse{
				Value: value,
			}
		case raft_cmdpb.CmdType_Put:
			if !keyInRegion(request.Put.Key, d.Region()) {
				curr.cb.Done(ErrResp(
					&util.ErrKeyNotInRegion{
						Key:    request.Put.Key,
						Region: d.Region(),
					},
				))
				return false
			}
			kvWB.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
			response.Put = &raft_cmdpb.PutResponse{}
		case raft_cmdpb.CmdType_Delete:
			if !keyInRegion(request.Delete.Key, d.Region()) {
				curr.cb.Done(ErrResp(
					&util.ErrKeyNotInRegion{
						Key:    request.Delete.Key,
						Region: d.Region(),
					},
				))
				return false
			}
			kvWB.DeleteCF(request.Delete.Cf, request.Delete.Key)
			response.Delete = &raft_cmdpb.DeleteResponse{}
		case raft_cmdpb.CmdType_Snap:
			response.Snap = &raft_cmdpb.SnapResponse{
				Region: d.peerStorage.region,
			}
			if curr.cb.Txn != nil {
				for _, req_ref := range cmd.Requests {
					log.Infof("%v", *req_ref)
				}
				panic("curr.cb.Txn != nil")
			}
			curr.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
		}
		responses[i] = response
	}

	admin_req := cmd.AdminRequest
	if admin_req != nil {
		admin_resp := d.applyAdminRequestWithResp(admin_req, kvWB)
		resp.AdminResponse = admin_resp
	}

	err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		BindRespError(resp, err)
	}
	kvWB.Reset()

	resp.Responses = responses

	curr.cb.Done(resp)

	if curr_i+1 < len(d.proposals) {
		d.proposals = append(d.proposals[0:curr_i], d.proposals[curr_i+1:]...)
	} else {
		d.proposals = d.proposals[0:curr_i]
	}
	return false
}

func (d *peerMsgHandler) process(ready *raft.Ready) {
	if ready.CommittedEntries != nil && len(ready.CommittedEntries) != 0 {
		var err error
		if d.IsLeader() {
			i := 0
			for ; i < len(ready.CommittedEntries) && ready.CommittedEntries[i].Term < d.Term(); i++ {
				if d.apply(&ready.CommittedEntries[i]) {
					return
				}
			}
			for ; i < len(ready.CommittedEntries); i++ {
				if d.applyWithResp(&ready.CommittedEntries[i]) {
					return
				}
			}
		} else {
			for _, entry := range ready.CommittedEntries {
				if d.apply(&entry) {
					return
				}
			}
		}
		apply := d.peerStorage.applyState
		if err != nil {
			panic(err.Error())
		}
		kvWB := &engine_util.WriteBatch{}
		apply.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
		err = kvWB.SetMeta(meta.ApplyStateKey(d.peerStorage.region.Id), apply)
		if err != nil {
			panic(err.Error())
		}
		err = kvWB.WriteToDB(d.peerStorage.Engines.Kv)
		if err != nil {
			panic(err.Error())
		}
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	var err error

	start := time.Now()
	defer func() {
		end := time.Now()
		elapsed := end.Sub(start)
		log.Debugf("%v handle raft ready takes %v", d.Tag, elapsed)
	}()

	ready := d.RaftGroup.Ready()

	if d.peerStorage.raftState.HardState.Term != ready.Term {
		for i := 0; i < len(d.proposals); {
			proposal := d.proposals[i]
			if proposal.term < ready.Term {
				proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
				if i+1 < len(d.proposals) {
					d.proposals = append(d.proposals[0:i], d.proposals[i+1:]...)
				} else {
					d.proposals = d.proposals[0:i]
				}
			} else {
				i++
			}
		}
	}

	result, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic(err.Error())
	}

	if result != nil {
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regions[result.Region.Id] = result.Region
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{result.Region})
		d.ctx.storeMeta.Unlock()
		log.Infof("%v store id %v snapshot", d.Tag, d.storeID())
		log.Infof("prev region: %v, curr region: %v", result.PrevRegion, result.Region)
	}

	snapshot := &eraftpb.Snapshot{}
	for i, msg := range ready.Messages {
		if msg.MsgType != eraftpb.MessageType_MsgSnapshot || msg.Snapshot != nil {
			continue
		}
		if snapshot.Data != nil && len(snapshot.Data) != 0 {
			msg.Snapshot = snapshot
		}
		for err = raft.ErrSnapshotTemporarilyUnavailable; err != nil; time.Sleep(1 * time.Microsecond) {
			if err != raft.ErrSnapshotTemporarilyUnavailable {
				panic(err.Error())
			}
			*snapshot, err = d.peerStorage.Snapshot()
		}
		ready.Messages[i].Snapshot = snapshot
	}

	d.Send(d.ctx.trans, ready.Messages)

	d.process(&ready)

	if !d.IsLeader() {
		for _, proposal := range d.proposals {
			proposal.cb.Done(ErrResp(&util.ErrStaleCommand{}))
		}
		d.proposals = make([]*proposal, 0)
	}

	d.RaftGroup.Advance(ready)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	log.Debugf("%v receives msg %v", d.Tag, msg)
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func keyInRegion(key []byte, region *metapb.Region) bool {
	return bytes.Compare(key, region.StartKey) >= 0 &&
		(len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0)
}

// return true if command is read-only and has been processed successfully
// otherwise need to propose, commit and apply
func (d *peerMsgHandler) readWithoutCommit(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
	if !d.RaftGroup.Raft.Readable() || msg.AdminRequest != nil {
		return false
	}

	is_all_read_req := true
	for _, req := range msg.Requests {
		if req.CmdType != raft_cmdpb.CmdType_Get && req.CmdType != raft_cmdpb.CmdType_Snap {
			is_all_read_req = false
			break
		}
	}
	if !is_all_read_req {
		return false
	}

	// process io-related reqeust asynchronously
	go func() {

		resps := make([]*raft_cmdpb.Response, len(msg.Requests))

		err := d.ctx.engine.Kv.View(func(txn *badger.Txn) error {
			for i, req := range msg.Requests {
				switch req.CmdType {
				case raft_cmdpb.CmdType_Get:
					if !keyInRegion(req.Get.GetKey(), d.Region()) {
						cb.Done(ErrResp(&util.ErrKeyNotInRegion{
							Key:    req.Get.GetKey(),
							Region: d.Region(),
						}))
						continue
					}
					key := engine_util.KeyWithCF(req.Get.Cf, req.Get.Key)
					item, err := txn.Get(key)
					if err != nil {
						if err == badger.ErrKeyNotFound {
							resps[i] = &raft_cmdpb.Response{
								CmdType: raft_cmdpb.CmdType_Get,
								Get: &raft_cmdpb.GetResponse{
									Value: make([]byte, 0),
								},
							}
							return nil
						}
						return err
					}
					val, err := item.Value()
					if err != nil {
						return err
					}
					resps[i] = &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Get,
						Get: &raft_cmdpb.GetResponse{
							Value: val,
						},
					}
				case raft_cmdpb.CmdType_Snap:
					if cb.Txn != nil {
						panic("Two Snap command in one request!")
					}
					cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
					resps[i] = &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Snap,
						Snap: &raft_cmdpb.SnapResponse{
							Region: d.peerStorage.region,
						},
					}
				default:
					panic("Not a read-only command!")
				}
			}
			return nil
		})

		if err != nil {
			panic(err.Error())
		}

		resp := newCmdResp()
		resp.Responses = resps

		cb.Done(resp)
	}()

	return true
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if d.readWithoutCommit(msg, cb) {
		return
	}

	proposal := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}

	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		if msg.Header.RegionEpoch.ConfVer < d.Region().RegionEpoch.ConfVer {
			cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
		}
		ctx := eraftpb.ConfChangeContext{
			StoreId: msg.AdminRequest.ChangePeer.Peer.StoreId,
			Confver: msg.Header.RegionEpoch.ConfVer,
		}
		data, err := ctx.Marshal()
		if err != nil {
			panic(err.Error())
		}
		cc := eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			Context:    data,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
		}
		err = d.RaftGroup.ProposeConfChange(cc)
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		d.proposals = append(d.proposals, proposal)
		log.Infof("confver %v Recieved conf change %v", d.Region().RegionEpoch.ConfVer, msg.AdminRequest.ChangePeer)
		return
	}

	data, err := msg.Marshal()
	if err != nil {
		panic(err.Error())
	}

	err = d.RaftGroup.Propose(data)
	if err == raft.ErrProposalDropped {
		if msg.AdminRequest != nil &&
			msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader &&
			msg.AdminRequest.TransferLeader.Peer.Id == d.RaftGroup.Raft.LeadTransferee() {
			resp := &raft_cmdpb.RaftCmdResponse{
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				}}
			BindRespTerm(resp, d.Term())
			cb.Done(resp)
			return
		}
		cb.Done(ErrResp(err))
		return
	} else if err != nil {
		panic(err.Error())
	}

	d.proposals = append(d.proposals, proposal)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	// log.Debugf("%s handle raft message %s from %d to %d",
	// 	d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	to := msg.GetToPeer()
	// log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
