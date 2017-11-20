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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

type balanceCellScheduler struct {
	cfg      *Cfg
	cache    *idCache
	limit    uint64
	selector Selector
}

func newBalanceCellScheduler(cfg *Cfg) *balanceCellScheduler {
	cache := newIDCache(storeCacheInterval, 4*storeCacheInterval)

	var filters []Filter
	filters = append(filters, newCacheFilter(cache))
	filters = append(filters, newStateFilter(cfg))
	filters = append(filters, newHealthFilter(cfg))
	filters = append(filters, newSnapshotCountFilter(cfg))

	return &balanceCellScheduler{
		cfg:      cfg,
		cache:    cache,
		limit:    1,
		selector: newBalanceSelector(cellKind, filters),
	}
}

func (s *balanceCellScheduler) GetName() string {
	return "balance-cell-scheduler"
}

func (s *balanceCellScheduler) GetResourceKind() ResourceKind {
	return cellKind
}

func (s *balanceCellScheduler) GetResourceLimit() uint64 {
	return minUint64(s.limit, s.cfg.LimitScheduleCell)
}

func (s *balanceCellScheduler) Prepare(cache *cache) error { return nil }

func (s *balanceCellScheduler) Cleanup(cache *cache) {}

func (s *balanceCellScheduler) Schedule(cache *cache) Operator {
	// Select a peer from the store with most cells.
	cell, oldPeer := scheduleRemovePeer(cache, s.selector)
	if cell == nil {
		return nil
	}

	// We don't schedule cell with abnormal number of replicas.
	if len(cell.getPeers()) != int(s.cfg.LimitReplicas) {
		return nil
	}

	op := s.transferPeer(cache, cell, oldPeer)
	if op == nil {
		// We can't transfer peer from this store now, so we add it to the cache
		// and skip it for a while.
		s.cache.set(oldPeer.StoreID)
	}
	return op
}

func (s *balanceCellScheduler) transferPeer(cache *cache, cell *CellInfo, oldPeer *metapb.Peer) Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	stores := cache.getStoreCache().getCellStores(cell)
	source := cache.getStoreCache().getStore(oldPeer.StoreID)
	scoreGuard := newDistinctScoreFilter(s.cfg, stores, source)

	checker := newReplicaChecker(s.cfg, cache)
	newPeer, _ := checker.selectBestPeer(cell, true, scoreGuard)
	if newPeer == nil {
		return nil
	}

	target := cache.getStoreCache().getStore(newPeer.StoreID)
	if !shouldBalance(source, target, s.GetResourceKind()) {
		return nil
	}
	s.limit = adjustBalanceLimit(cache, s.GetResourceKind())

	return newTransferPeerAggregationOp(cell, oldPeer, newPeer)
}
