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
	"math"

	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
)

const replicaBaseScore = 100

type replicaChecker struct {
	cfg     *Cfg
	cache   *cache
	filters []Filter
}

func newReplicaChecker(cfg *Cfg, cache *cache, filters ...Filter) *replicaChecker {
	return &replicaChecker{
		cache:   cache,
		cfg:     cfg,
		filters: filters,
	}
}

// Check return the Operator
func (r *replicaChecker) Check(target *CellInfo) Operator {
	if op := r.checkDownPeer(target); op != nil {
		return op
	}

	if op := r.checkOfflinePeer(target); op != nil {
		return op
	}

	currReplicasCount := uint32(len(target.getPeers()))

	if currReplicasCount < r.cfg.Schedule.MaxReplicas {
		newPeer, _ := r.selectBestPeer(target, true, r.filters...)
		if newPeer == nil {
			return nil
		}

		return newAddPeerAggregationOp(target, newPeer)
	}

	if currReplicasCount > r.cfg.Schedule.MaxReplicas {
		oldPeer, _ := r.selectWorstPeer(target)
		if oldPeer == nil {
			return nil
		}

		return newRemovePeerOp(target.getID(), oldPeer)
	}

	return r.checkBestReplacement(target)
}

func (r *replicaChecker) checkDownPeer(cell *CellInfo) Operator {
	for _, stats := range cell.DownPeers {
		peer := stats.Peer
		store := r.cache.getStoreCache().getStore(peer.StoreID)

		if nil != store && store.downTime() < r.cfg.Schedule.getMaxStoreDownTimeDuration() {
			continue
		}

		if nil != store && stats.DownSeconds < uint64(r.cfg.Schedule.getMaxStoreDownTimeDuration().Seconds()) {
			continue
		}

		return newRemovePeerOp(cell.getID(), &peer)
	}

	return nil
}

func (r *replicaChecker) checkOfflinePeer(cell *CellInfo) Operator {
	for _, peer := range cell.Meta.Peers {
		store := r.cache.getStoreCache().getStore(peer.StoreID)

		if store != nil && store.isUp() {
			continue
		}

		newPeer, _ := r.selectBestPeer(cell, true)
		if newPeer == nil {
			return nil
		}

		return newTransferPeerAggregationOp(cell, peer, newPeer)
	}

	return nil
}

// selectWorstPeer returns the worst peer in the cell.
func (r *replicaChecker) selectWorstPeer(cell *CellInfo, filters ...Filter) (*meta.Peer, float64) {
	var (
		worstStore *StoreInfo
		worstScore float64
	)

	// Select the store with lowest distinct score.
	// If the scores are the same, select the store with maximal cell score.
	stores := r.cache.getStoreCache().getCellStores(cell)
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		score := r.cfg.getDistinctScore(stores, store)
		if worstStore == nil || compareStoreScore(r.cfg, store, score, worstStore, worstScore) < 0 {
			worstStore = store
			worstScore = score
		}
	}

	if worstStore == nil || filterSource(worstStore, r.filters) {
		return nil, 0
	}

	return cell.getStorePeer(worstStore.getID()), worstScore
}

// selectBestPeer returns the best peer in other stores.
func (r *replicaChecker) selectBestPeer(target *CellInfo, allocPeerID bool, filters ...Filter) (*meta.Peer, float64) {
	// Add some must have filters.
	filters = append(filters, newStateFilter(r.cfg))
	filters = append(filters, newStorageThresholdFilter(r.cfg))
	filters = append(filters, newExcludedFilter(nil, target.getStoreIDs()))

	var (
		bestStore *StoreInfo
		bestScore float64
	)

	// Select the store with best distinct score.
	// If the scores are the same, select the store with minimal cells score.
	stores := r.cache.getStoreCache().getCellStores(target)
	for _, store := range r.cache.getStoreCache().getStores() {
		if filterTarget(store, filters) {
			continue
		}

		score := getDistinctScore(r.cfg, stores, store)
		if bestStore == nil || compareStoreScore(r.cfg, store, score, bestStore, bestScore) > 0 {
			bestStore = store
			bestScore = score
		}
	}

	if bestStore == nil || filterTarget(bestStore, r.filters) {
		return nil, 0
	}

	newPeer, err := r.cache.allocPeer(bestStore.getID(), allocPeerID)
	if err != nil {
		log.Errorf("scheduler: allocate peer failure, errors:\n %+v", err)
		return nil, 0
	}
	return &newPeer, bestScore
}

func (r *replicaChecker) checkBestReplacement(cell *CellInfo) Operator {
	oldPeer, oldScore := r.selectWorstPeer(cell)
	if oldPeer == nil {
		return nil
	}
	newPeer, newScore := r.selectBestReplacement(cell, oldPeer)
	if newPeer == nil {
		return nil
	}

	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}

	id, err := r.cache.allocator.newID()
	if err != nil {
		log.Errorf("scheduler: allocate peer failure, errors:\n %+v", err)
		return nil
	}

	newPeer.ID = id
	return newTransferPeerAggregationOp(cell, oldPeer, newPeer)
}

// selectBestReplacement returns the best peer to replace the cell peer.
func (r *replicaChecker) selectBestReplacement(cell *CellInfo, peer *meta.Peer) (*meta.Peer, float64) {
	// selectBestReplacement returns the best peer to replace the cell peer.
	// Get a new cell without the peer we are going to replace.
	newCell := cell.clone()
	newCell.removeStorePeer(peer.StoreID)

	return r.selectBestPeer(newCell, false, newExcludedFilter(nil, cell.getStoreIDs()))
}

// getDistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func getDistinctScore(cfg *Cfg, stores []*StoreInfo, other *StoreInfo) float64 {
	score := float64(0)
	locationLabels := cfg.getLocationLabels()

	for i := range locationLabels {
		keys := locationLabels[0 : i+1]
		level := len(locationLabels) - i - 1
		levelScore := math.Pow(replicaBaseScore, float64(level))

		for _, s := range stores {
			if s.getID() == other.getID() {
				continue
			}
			id1 := s.getLocationID(keys)
			if len(id1) == 0 {
				return 0
			}
			id2 := other.getLocationID(keys)
			if len(id2) == 0 {
				return 0
			}
			if id1 != id2 {
				score += levelScore
			}
		}
	}

	return score
}

// compareStoreScore compares which store is better for replication.
// Returns 0 if store A is as good as store B.
// Returns 1 if store A is better than store B.
// Returns -1 if store B is better than store A.
func compareStoreScore(cfg *Cfg, storeA *StoreInfo, scoreA float64, storeB *StoreInfo, scoreB float64) int {
	// The store with higher score is better.
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	// The store with lower region score is better.
	if storeA.cellScore() < storeB.cellScore() {
		return 1
	}
	if storeA.cellScore() > storeB.cellScore() {
		return -1
	}
	return 0
}
