// Copyright 2019 PingCAP, Inc.
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

package core

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
)

// StoreCreateOption is used to create store.
type StoreCreateOption func(region *StoreInfo)

// SetStoreAddress sets the address for the store.
func SetStoreAddress(address string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.Meta).(*metapb.Store)
		meta.Address = address
		store.Meta = meta
	}
}

// SetStoreState sets the state for the store.
func SetStoreState(state metapb.StoreState) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.Meta).(*metapb.Store)
		meta.State = state
		store.Meta = meta
	}
}

// SetStoreBlock stops balancer from selecting the store.
func SetStoreBlock() StoreCreateOption {
	return func(store *StoreInfo) {
		store.Blocked = true
	}
}

// SetStoreUnBlock allows balancer to select the store.
func SetStoreUnBlock() StoreCreateOption {
	return func(store *StoreInfo) {
		store.Blocked = false
	}
}

// SetLeaderCount sets the leader count for the store.
func SetLeaderCount(leaderCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.LeaderCount = leaderCount
	}
}

// SetRegionCount sets the Region count for the store.
func SetRegionCount(regionCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.RegionCount = regionCount
	}
}

// SetPendingPeerCount sets the pending peer count for the store.
func SetPendingPeerCount(pendingPeerCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.PendingPeerCount = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the store.
func SetLeaderSize(leaderSize int64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.LeaderSize = leaderSize
	}
}

// SetRegionSize sets the Region size for the store.
func SetRegionSize(regionSize int64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.RegionSize = regionSize
	}
}

// SetLeaderWeight sets the leader weight for the store.
func SetLeaderWeight(leaderWeight float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.LeaderWeight = leaderWeight
	}
}

// SetRegionWeight sets the Region weight for the store.
func SetRegionWeight(regionWeight float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.RegionWeight = regionWeight
	}
}

// SetLastHeartbeatTS sets the time of last heartbeat for the store.
func SetLastHeartbeatTS(lastHeartbeatTS time.Time) StoreCreateOption {
	return func(store *StoreInfo) {
		store.LastHeartbeatTS = lastHeartbeatTS
	}
}

// SetStoreStats sets the statistics information for the store.
func SetStoreStats(stats *schedulerpb.StoreStats) StoreCreateOption {
	return func(store *StoreInfo) {
		store.Stats = stats
	}
}

// SetAvailableFunc sets a customize function for the store. The function f returns true if the store limit is not exceeded.
func SetAvailableFunc(f func() bool) StoreCreateOption {
	return func(store *StoreInfo) {
		store.available = f
	}
}
