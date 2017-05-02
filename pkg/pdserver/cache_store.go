// Copyright 2016 DeepFabric, Inc.
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

package pdserver

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

func newStoreRuntime(store metapb.Store) *storeRuntimeInfo {
	return &storeRuntimeInfo{
		store: store,
		status: &StoreStatus{
			stats: &pdpb.StoreStats{},
		},
	}
}

func newStoreCache() *storeCache {
	sc := new(storeCache)
	sc.stores = make(map[uint64]*storeRuntimeInfo)

	return sc
}

type storeCache struct {
	stores map[uint64]*storeRuntimeInfo
}

// StoreStatus contains information about a store's status.
type StoreStatus struct {
	stats *pdpb.StoreStats
	// Blocked means that the store is blocked from balance.
	blocked         bool
	LeaderCount     uint32    `json:"leader_count"`
	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`
}

type storeRuntimeInfo struct {
	store  metapb.Store
	status *StoreStatus
}

func (s *storeRuntimeInfo) getID() uint64 {
	return s.store.ID
}

func (s *storeRuntimeInfo) isUp() bool {
	return s.store.State == metapb.UP
}

func (s *storeRuntimeInfo) isTombstone() bool {
	return s.store.State == metapb.Tombstone
}

func (s *storeRuntimeInfo) isBlocked() bool {
	return s.status.blocked
}

func (s *storeRuntimeInfo) downTime() time.Duration {
	return time.Since(s.status.LastHeartbeatTS)
}

func (s *storeRuntimeInfo) resourceCount(kind ResourceKind) uint64 {
	switch kind {
	case leaderKind:
		return s.leaderCount()
	case cellKind:
		return s.cellCount()
	default:
		return 0
	}
}

func (s *storeRuntimeInfo) resourceScore(kind ResourceKind) float64 {
	switch kind {
	case leaderKind:
		return s.leaderScore()
	case cellKind:
		return s.cellScore()
	default:
		return 0
	}
}

func (s *storeRuntimeInfo) leaderCount() uint64 {
	return uint64(s.status.LeaderCount)
}

func (s *storeRuntimeInfo) leaderScore() float64 {
	return float64(s.status.LeaderCount)
}

func (s *storeRuntimeInfo) cellCount() uint64 {
	return uint64(s.status.stats.CellCount)
}

func (s *storeRuntimeInfo) cellScore() float64 {
	if s.status.stats.Capacity == 0 {
		return 0
	}
	return float64(s.status.stats.CellCount) / float64(s.status.stats.Capacity)
}

func (s *storeRuntimeInfo) storageRatio() int {
	cap := s.status.stats.Capacity

	if cap == 0 {
		return 0
	}

	return int(float64(s.storageSize()) * 100 / float64(cap))
}

func (s *storeRuntimeInfo) storageSize() uint64 {
	return s.status.stats.Capacity - s.status.stats.Available
}

func (s *storeRuntimeInfo) getLocationID(keys []string) string {
	id := ""
	for _, k := range keys {
		v := s.getLabelValue(k)
		if len(v) == 0 {
			return ""
		}
		id += v
	}
	return id
}

func (s *storeRuntimeInfo) getLabelValue(key string) string {
	for _, label := range s.store.Lables {
		if label.Key == key {
			return label.Value
		}
	}

	return ""
}
