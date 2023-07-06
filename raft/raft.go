// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// heartbeat responses accepted count
	// used to check whether it is safe to read data without commit entries
	heartbeatsReceived int

	// whether it is safe to read data without commit entries
	readable bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	RaftLog := newLog(c.Storage)

	Prs := make(map[uint64]*Progress)
	for _, pr := range c.peers {
		Prs[pr] = &Progress{
			Match: RaftLog.logIndex(),
			Next:  RaftLog.logIndex() + 1,
		}
	}

	istate, cstate, err := c.Storage.InitialState()
	for _, pr := range cstate.Nodes {
		Prs[pr] = &Progress{
			Match: RaftLog.logIndex(),
			Next:  RaftLog.logIndex() + 1,
		}
	}

	if err != nil {
		panic(err.Error())
	}

	return &Raft{
		Term: istate.Term,
		Vote: istate.Vote,

		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,

		RaftLog: RaftLog,
		Prs:     Prs,

		State: StateFollower,

		electionElapsed: c.ElectionTick,

		votes: make(map[uint64]bool),
		msgs:  make([]pb.Message, 0),

		// 3ATODO
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
}

func (r *Raft) Readable() bool {
	return r.readable
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	assert(r.State == StateLeader)

	next := r.Prs[to].Next
	if len(r.RaftLog.entries) == 0 || next > r.RaftLog.entries[len(r.RaftLog.entries)-1].Index {
		return false
	} else if next < r.RaftLog.entries[0].Index {
		r.sendSnapshot(to)
		return false
	}

	msg := r.newMessage(pb.MessageType_MsgAppend, to)
	msg.Commit = r.RaftLog.committed
	msg.Entries = r.RaftLog.getEntriesRef(next)
	msg.Index = next - 1

	term, err := r.RaftLog.Term(msg.Index)
	if err != nil {
		panic(err.Error())
	}

	msg.LogTerm = term

	r.send(msg)

	r.Prs[to].Next = r.RaftLog.logIndex() + 1

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := r.newMessage(pb.MessageType_MsgHeartbeat, to)
	msg.Commit = r.RaftLog.committed
	msg.Index = r.RaftLog.logIndex()
	msg.LogTerm = r.RaftLog.logTerm()
	r.send(msg)
}

func (r *Raft) sendSnapshot(to uint64) {
	// 2C
	snapshot := &pb.Snapshot{}
	var err error
	// for err = ErrSnapshotTemporarilyUnavailable; err == ErrSnapshotTemporarilyUnavailable; time.Sleep(1 * time.Microsecond) {
	// }
	// if err != nil {
	// 	panic(err)
	// }
	*snapshot, err = r.RaftLog.storage.Snapshot()
	if err != nil {
		snapshot = nil
	}

	msg := r.newMessage(pb.MessageType_MsgSnapshot, to)
	msg.Snapshot = snapshot

	r.send(msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		if r.heartbeatElapsed++; r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.bcastHeartbeatZero()
			// msg := r.newMessage(pb.MessageType_MsgBeat, r.id)
			// r.send(msg)
		}
	default:
		if r.electionElapsed++; r.electionElapsed >= r.electionTimeout {
			if r.electionElapsed >= r.electionTimeout*2 {
				r.electionElapsed = 0
				r.becomeCandidate()
				r.elect()
				return
			}
			// to randomize eletion timeout without add new member to struct `Raft`
			// possibility of elect is 1/n in each turn
			n := r.electionTimeout*2 - r.electionElapsed

			rd := rand.Int31()
			if int(rd)%n == 0 {
				r.electionElapsed = 0
				r.becomeCandidate()
				r.elect()
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	if term > r.Term {
		r.Term = term
		r.Vote = 0
	}
}

func (r *Raft) elect() {
	r.electionElapsed = 0

	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.bcastHeartbeat()
		return
	}

	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

	// send RequestVote
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		msg := r.newMessage(pb.MessageType_MsgRequestVote, pr)
		msg.Index = r.RaftLog.logIndex()
		msg.LogTerm = r.RaftLog.logTerm()
		r.send(msg)
	}

}

func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.readable = false
	r.heartbeatsReceived = 0
	r.heartbeatElapsed = r.heartbeatTimeout

	for _, progress := range r.Prs {
		progress.Match = 0
		progress.Next = r.RaftLog.logIndex() + 1
	}

	r.handlePropose(pb.Message{
		Entries: []*pb.Entry{{}},
	})
}

func (r *Raft) send(msg *pb.Message) {
	r.msgs = append(r.msgs, *msg)
}

func (r *Raft) bcastHeartbeat() {
	for pr := range r.Prs {
		if pr != r.id {
			r.sendHeartbeat(pr)
		}
	}
	r.heartbeatElapsed = 0
	if r.heartbeatsReceived+1 > len(r.Prs)/2 {
		r.readable = true
	} else {
		r.readable = false
	}
	r.heartbeatsReceived = 0
}

func (r *Raft) bcastHeartbeatZero() {
	for pr := range r.Prs {
		if pr != r.id {
			msg := r.newMessage(pb.MessageType_MsgHeartbeat, pr)
			r.send(msg)
		}
	}
	r.heartbeatElapsed = 0
	if r.heartbeatsReceived+1 > len(r.Prs)/2 {
		r.readable = true
	} else {
		r.readable = false
	}
	r.heartbeatsReceived = 0
}

func (r *Raft) bcastAppend() {
	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}
}

func (r *Raft) isFromLeader(m *pb.Message) bool {
	if r.Term > m.Term {
		return false
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		return true
	case pb.MessageType_MsgSnapshot:
		return true
	case pb.MessageType_MsgHeartbeat:
		return true
	default:
		return false
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
			}
		default:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, None)
			}
		}
	default:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
	}

	if r.isFromLeader(&m) {
		r.Lead = m.From
	}

	r.callHandler(m)

	// for _, pr := range r.Prs {
	// 	if pr.Next == 0 {
	// 		panic("")
	// 	}
	// }

	return nil
}

func (r *Raft) callHandler(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	// 2CTODO
	default:
		log.Fatalf("Unimplemented message type: %v", m.MsgType)
	}
}

func (r Raft) newMessage(mtype pb.MessageType, to uint64) *pb.Message {
	return &pb.Message{
		From:    r.id,
		To:      to,
		MsgType: mtype,
		Term:    r.Term,
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	msg := r.newMessage(pb.MessageType_MsgRequestVoteResponse, m.From)
	if m.Term >= r.Term &&
		(r.Vote == 0 || r.Vote == m.From) &&
		// candidate's log is at least as up-to-date as receiver's log
		(m.LogTerm > r.RaftLog.logTerm() ||
			(m.LogTerm == r.RaftLog.logTerm() && m.Index >= r.RaftLog.logIndex())) {
		r.Vote = m.From
	} else {
		msg.Reject = true
	}

	r.msgs = append(r.msgs, *msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if r.State == StateCandidate {
		r.votes[m.From] = !m.Reject

		count := 0
		for _, grant := range r.votes {
			if grant {
				count++
			}
		}

		if count > len(r.Prs)/2 {
			r.becomeLeader()
			r.bcastHeartbeat()
		} else if len(r.votes) == len(r.Prs) {
			r.State = StateFollower
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State == StateCandidate {
		return
	}
	if r.State == StateFollower {
		// redirect to leader
		if r.Lead != 0 {
			m.To = r.Lead
			r.send(&m)
		}
		return
	}

	log.Debugf("%v handle propose", r.id)

	index := r.RaftLog.logIndex() + 1
	for i, entry_ref := range m.Entries {
		entry := *entry_ref
		entry.Index = uint64(i) + index
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, entry)
	}

	index = r.RaftLog.logIndex()

	r.Prs[r.id] = &Progress{
		Match: index,
		Next:  index + 1,
	}

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.logIndex()
		return
	}

	r.bcastAppend()
}

func (r *Raft) updateCommit(m pb.Message) {
	last_new_index := m.Index + uint64(len(m.Entries))

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, last_new_index)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := r.newMessage(pb.MessageType_MsgAppendResponse, m.From)

	defer r.send(msg)

	if len(m.Entries) != 0 && len(m.Entries[0].Data) != 0 {
		log.Debugf("debug")
	}

	if m.Term < r.Term {
		msg.Reject = true
		return
	}

	r.electionElapsed = 0

	if !(m.Index == 0 && m.LogTerm == 0) {
		term, err := r.RaftLog.Term(m.Index)
		if err != nil {
			if err == ErrUnavailable {
				msg.Reject = true
				msg.Index = r.RaftLog.logIndex()
				return
			}
			panic(err.Error())
		}
		if term != m.LogTerm {
			msg.Reject = true
			msg.Index = m.Index - 1
			return
		}
	}

	defer r.updateCommit(m)

	if len(m.Entries) == 0 {
		msg.Index = r.RaftLog.logIndex()
		return
	}

	moffset := m.Entries[0].Index
	offset := r.RaftLog.offset()
	if moffset > r.RaftLog.logIndex()+1 {
		msg.Reject = true
		msg.Index = r.RaftLog.logIndex()
		return
	}

	msg.Index = m.Index + uint64(len(m.Entries))

	var overlap_m_lo, overlap_lo uint64 = 0, 0
	if offset > moffset {
		overlap_m_lo = offset - moffset
	} else {
		overlap_lo = moffset - offset
	}

	m_append_offset := r.RaftLog.logIndex() + 1 - moffset

	if m_append_offset > uint64(len(m.Entries)) {
		m_append_offset = uint64(len(m.Entries))
		for i, entry_ref := range m.Entries[overlap_m_lo:m_append_offset] {
			assert(r.RaftLog.entries[overlap_lo+uint64(i)].Index == entry_ref.Index)
			if r.RaftLog.entries[overlap_lo+uint64(i)].Term != entry_ref.Term {
				r.RaftLog.entries = r.RaftLog.entries[:overlap_lo+uint64(i)]
				r.RaftLog.stabled = r.RaftLog.logIndex()
				for _, entry := range m.Entries[i+int(overlap_m_lo):] {
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}
				return
			}
		}
		return
	}

	for i, entry_ref := range m.Entries[overlap_m_lo:m_append_offset] {
		assert(r.RaftLog.entries[overlap_lo+uint64(i)].Index == entry_ref.Index)
		if r.RaftLog.entries[overlap_lo+uint64(i)].Term != entry_ref.Term {
			r.RaftLog.entries = r.RaftLog.entries[:overlap_lo+uint64(i)]
			r.RaftLog.stabled = r.RaftLog.logIndex()
			for _, entry := range m.Entries[i+int(overlap_m_lo):] {
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			}
			return
		}
	}

	for _, entry_ref := range m.Entries[m_append_offset:] {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry_ref)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}

	if m.Reject {
		r.Prs[m.From] = &Progress{
			Next: m.Index + 1,
		}
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From] = &Progress{
		Match: m.Index,
		Next:  m.Index + 1,
	}

	matches := make([]uint64, len(r.Prs))
	i := 0
	for _, p := range r.Prs {
		matches[i] = p.Match
		i++
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i] < matches[j]
	})

	// len(r.Prs) >= 2
	majority_index := (len(r.Prs) - 1) / 2
	next_committed := matches[majority_index]
	if next_committed > r.RaftLog.committed && r.RaftLog.getEntry(next_committed).Term == r.Term {
		r.RaftLog.committed = next_committed

		for pr := range r.Prs {
			if pr != r.id {
				msg := r.newMessage(pb.MessageType_MsgAppend, pr)
				msg.Commit = next_committed
				msg.Index = r.RaftLog.logIndex()
				msg.LogTerm = r.RaftLog.logTerm()
				r.send(msg)
			}
		}
	}
}

func (r *Raft) handleHup(m pb.Message) {
	if r.State == StateFollower || r.State == StateCandidate {
		r.becomeCandidate()
		r.elect()
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	if r.State == StateLeader {
		r.bcastHeartbeat()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := r.newMessage(pb.MessageType_MsgHeartbeatResponse, m.From)
	defer r.send(msg)
	r.electionElapsed = 0

	if m.Index > r.RaftLog.logIndex() || m.Index < r.RaftLog.truncatedIndex {
		msg.Reject = true
		msg.Index = r.RaftLog.logIndex()
		return
	}
	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err.Error())
	}
	if term != m.LogTerm {
		msg.Reject = true
		msg.Index = m.Index - 1
	} else {
		r.updateCommit(m)
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	if m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.sendAppend(m.From)
	} else {
		r.heartbeatsReceived++
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term < r.Term || r.RaftLog.applied >= m.Snapshot.Metadata.Index {
		return
	} else if r.State == StateLeader {
		panic("Leader received snapshot from the same term!")
	}
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.committed = max(r.RaftLog.applied, r.RaftLog.committed)
	r.RaftLog.stabled = max(r.RaftLog.committed, r.RaftLog.stabled)
	r.RaftLog.truncatedIndex = m.Snapshot.Metadata.Index
	r.RaftLog.truncatedTerm = m.Snapshot.Metadata.Term

	// 3C conf change
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
