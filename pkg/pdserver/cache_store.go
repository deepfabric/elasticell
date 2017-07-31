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
	"errors"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

func newStoreInfo(store metapb.Store) *StoreInfo {
	return &StoreInfo{
		Meta: store,
		Status: &StoreStatus{
			Stats: &pdpb.StoreStats{},
		},
	}
}

func newStoreCache() *storeCache {
	sc := new(storeCache)
	sc.stores = make(map[uint64]*StoreInfo)

	return sc
}

type storeCache struct {
	sync.RWMutex
	stores map[uint64]*StoreInfo
}

func (sc *storeCache) createStoreInfo(store metapb.Store) {
	sc.Lock()
	defer sc.Unlock()

	if _, ok := sc.stores[store.ID]; ok {
		return
	}

	sc.stores[store.ID] = newStoreInfo(store)
}

func (sc *storeCache) updateStoreInfo(store *StoreInfo) {
	sc.Lock()
	defer sc.Unlock()
	sc.stores[store.Meta.ID] = store
}

func (sc *storeCache) getStores() []*StoreInfo {
	sc.RLock()
	defer sc.RUnlock()

	stores := make([]*StoreInfo, 0, len(sc.stores))
	for _, store := range sc.stores {
		stores = append(stores, store.clone())
	}
	return stores
}

func (sc *storeCache) getStore(storeID uint64) *StoreInfo {
	sc.RLock()
	defer sc.RUnlock()

	return sc.getStoreWithoutLock(storeID)
}

func (sc *storeCache) setStoreOffline(storeID uint64) (*StoreInfo, error) {
	sc.Lock()
	defer sc.Unlock()

	store := sc.getStoreWithoutLock(storeID)
	if nil == store {
		return nil, errStoreNotFound
	}

	if store.isOffline() {
		return nil, nil
	}

	if store.isTombstone() {
		return nil, errors.New("store has been removed")
	}

	store.Meta.State = metapb.Down
	return store, nil
}

func (sc *storeCache) setStoreTombstone(storeID uint64, force bool) (*StoreInfo, error) {
	sc.Lock()
	defer sc.Unlock()

	store := sc.getStoreWithoutLock(storeID)
	if nil == store {
		return nil, errStoreNotFound
	}

	if store.isTombstone() {
		return nil, nil
	}

	if store.isUp() {
		if !force {
			return nil, errors.New("store is still up, please remove store gracefully")
		}
	}

	store.Meta.State = metapb.Tombstone
	store.Status = newStoreStatus()
	return store, nil
}

func (sc *storeCache) getCellStores(cell *CellInfo) []*StoreInfo {
	sc.RLock()
	defer sc.RUnlock()

	var stores []*StoreInfo
	for id := range cell.getStoreIDs() {
		if store := sc.getStoreWithoutLock(id); store != nil {
			stores = append(stores, store.clone())
		}
	}
	return stores
}

func (sc *storeCache) getFollowerStores(cell *CellInfo) []*StoreInfo {
	sc.RLock()
	defer sc.RUnlock()

	var stores []*StoreInfo
	for id := range cell.getFollowers() {
		if store := sc.getStoreWithoutLock(id); store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (sc *storeCache) foreach(fn func(*StoreInfo) (bool, error)) error {
	sc.RLock()
	defer sc.RUnlock()

	for _, s := range sc.stores {
		next, err := fn(s)
		if err != nil {
			return err
		}

		if !next {
			break
		}
	}

	return nil
}

func (sc *storeCache) getStoreWithoutLock(storeID uint64) *StoreInfo {
	store, ok := sc.stores[storeID]
	if !ok {
		return nil
	}
	return store.clone()
}

func newStoreStatus() *StoreStatus {
	return &StoreStatus{
		Stats: &pdpb.StoreStats{},
	}
}

// StoreStatus contains information about a store's status.
type StoreStatus struct {
	Stats           *pdpb.StoreStats
	LeaderCount     uint32
	LastHeartbeatTS time.Time

	// Blocked means that the store is blocked from balance.
	blocked bool
}

// StoreInfo store info
type StoreInfo struct {
	Meta   metapb.Store
	Status *StoreStatus
}

func (s *StoreInfo) getID() uint64 {
	return s.Meta.ID
}

func (s *StoreInfo) isUp() bool {
	return s.Meta.State == metapb.UP
}

func (s *StoreInfo) isTombstone() bool {
	return s.Meta.State == metapb.Tombstone
}

func (s *StoreInfo) isOffline() bool {
	return s.Meta.State == metapb.Down
}

func (s *StoreInfo) isBlocked() bool {
	return s.Status == nil || s.Status.blocked
}

func (s *StoreInfo) downTime() time.Duration {
	return time.Since(s.Status.LastHeartbeatTS)
}

func (s *StoreInfo) resourceCount(kind ResourceKind) uint64 {
	switch kind {
	case leaderKind:
		return s.leaderCount()
	case cellKind:
		return s.cellCount()
	default:
		return 0
	}
}

func (s *StoreInfo) resourceScore(kind ResourceKind) float64 {
	switch kind {
	case leaderKind:
		return s.leaderScore()
	case cellKind:
		return s.cellScore()
	default:
		return 0
	}
}

func (s *StoreInfo) leaderCount() uint64 {
	return uint64(s.Status.LeaderCount)
}

func (s *StoreInfo) leaderScore() float64 {
	return float64(s.Status.LeaderCount)
}

func (s *StoreInfo) cellCount() uint64 {
	return uint64(s.Status.Stats.CellCount)
}

func (s *StoreInfo) cellScore() float64 {
	if s.Status.Stats.Capacity == 0 {
		return 0
	}
	return float64(s.Status.Stats.CellCount) / float64(s.Status.Stats.Capacity)
}

func (s *StoreInfo) storageRatio() int {
	cap := s.Status.Stats.Capacity

	if cap == 0 {
		return 0
	}

	return int(float64(s.storageSize()) * 100 / float64(cap))
}

func (s *StoreInfo) storageSize() uint64 {
	return s.Status.Stats.Capacity - s.Status.Stats.Available
}

func (s *StoreInfo) getLocationID(keys []string) string {
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

func (s *StoreInfo) getLabelValue(key string) string {
	for _, label := range s.Meta.Lables {
		if label.Key == key {
			return label.Value
		}
	}

	return ""
}
