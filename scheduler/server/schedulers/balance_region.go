// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type byRegionSize []*core.StoreInfo

func (b byRegionSize) Len() int {
	return len(b)
}

func (b byRegionSize) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b byRegionSize) Less(i, j int) bool {
	return b[i].RegionSize > b[j].RegionSize
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	suitable_stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitable_stores = append(suitable_stores, store)
		}
	}

	sort.Sort(byRegionSize(suitable_stores))

	var region *core.RegionInfo = nil
	var from_store *core.StoreInfo

	for _, from_store = range suitable_stores {
		cluster.GetPendingRegionsWithLock(from_store.Meta.Id, func(c core.RegionsContainer) {
			region = c.RandomRegion([]byte(""), []byte(""))
		})

		if region == nil {
			cluster.GetFollowersWithLock(from_store.Meta.Id, func(c core.RegionsContainer) {
				region = c.RandomRegion([]byte(""), []byte(""))
			})
		}

		if region == nil {
			cluster.GetLeadersWithLock(from_store.Meta.Id, func(c core.RegionsContainer) {
				region = c.RandomRegion([]byte(""), []byte(""))
			})
		}

		if region != nil {
			break
		}
	}

	if region == nil {
		panic("Can't find region to move from!")
	}

	to_store := suitable_stores[len(suitable_stores)-1]

	is_valuable := from_store.RegionSize-to_store.RegionSize > 2*region.GetApproximateSize()
	if !is_valuable {
		return nil
	}

	op, err := operator.CreateMovePeerOperator("balance region", cluster, region, operator.OpBalance, from_store.GetID(), to_store.GetID(), region.GetStorePeer(from_store.GetID()).Id)
	if err != nil {
		panic(err)
	}

	return op
}
