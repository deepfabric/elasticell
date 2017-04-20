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

	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

// StoreStatus contains information about a store's status.
type StoreStatus struct {
	stats *pdpb.StoreStats
	// Blocked means that the store is blocked from balance.
	blocked         bool
	LeaderCount     uint32    `json:"leader_count"`
	LastHeartbeatTS time.Time `json:"last_heartbeat_ts"`
}

type storeRuntime struct {
	store  meta.Store
	status *StoreStatus
}

func newStoreRuntime(store meta.Store) *storeRuntime {
	return &storeRuntime{
		store: store,
		status: &StoreStatus{
			stats: &pdpb.StoreStats{},
		},
	}
}

func (s *storeRuntime) getID() uint64 {
	return s.store.ID
}

func (s *storeRuntime) isUp() bool {
	return s.store.State == meta.UP
}

func (s *storeRuntime) isTombstone() bool {
	return s.store.State == meta.Tombstone
}

func (s *storeRuntime) storageRatio() int {
	cap := s.status.stats.Capacity

	if cap == 0 {
		return 0
	}

	return int(float64(s.storageSize()) * 100 / float64(cap))
}

func (s *storeRuntime) storageSize() uint64 {
	return s.status.stats.Capacity - s.status.stats.Available
}

func (s *storeRuntime) cellScore() float64 {
	if s.status.stats.Capacity == 0 {
		return 0
	}

	return float64(s.status.stats.CellCount) / float64(s.status.stats.Capacity)
}

func (s *storeRuntime) getLocationID(keys []string) string {
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

func (s *storeRuntime) getLabelValue(key string) string {
	for _, label := range s.store.Lables {
		if label.Key == key {
			return label.Value
		}
	}

	return ""
}
