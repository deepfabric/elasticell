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
func (r *replicaChecker) Check(target *cellRuntimeInfo) Operator {
	if op := r.checkDownPeer(target); op != nil {
		return op
	}

	if op := r.checkOfflinePeer(target); op != nil {
		return op
	}

	if uint32(len(target.getPeers())) < r.cfg.getMaxReplicas() {
		newPeer, _ := r.selectBestPeer(target, r.filters...)
		if newPeer == nil {
			return nil
		}

		return newAddPeerAggregationOp(target, newPeer)
	}

	// TODO: impl
	// if len(region.GetPeers()) > r.rep.GetMaxReplicas() {
	// 	oldPeer, _ := r.selectWorstPeer(region)
	// 	if oldPeer == nil {
	// 		return nil
	// 	}
	// 	return newRemovePeer(region, oldPeer)
	// }

	// return r.checkBestReplacement(region)

	return nil
}

func (r *replicaChecker) checkDownPeer(cell *cellRuntimeInfo) Operator {
	for _, stats := range cell.downPeers {
		peer := stats.Peer
		store := r.cache.getStore(peer.StoreID)
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

func (r *replicaChecker) checkOfflinePeer(cell *cellRuntimeInfo) Operator {
	for _, peer := range cell.cell.Peers {
		store := r.cache.getStore(peer.ID)

		if store != nil && store.isUp() {
			continue
		}

		newPeer, _ := r.selectBestPeer(cell)
		if newPeer == nil {
			return nil
		}

		return newTransferPeerAggregationOp(cell, peer, newPeer)
	}

	return nil
}

// selectBestPeer returns the best peer in other stores.
func (r *replicaChecker) selectBestPeer(target *cellRuntimeInfo, filters ...Filter) (*meta.Peer, float64) {
	// Add some must have filters.
	filters = append(filters, newStateFilter(r.cfg))
	filters = append(filters, newStorageThresholdFilter(r.cfg))
	filters = append(filters, newExcludedFilter(nil, target.getStoreIDs()))

	var (
		bestStore *storeRuntimeInfo
		bestScore float64
	)

	// Select the store with best distinct score.
	// If the scores are the same, select the store with minimal cells score.
	stores := r.cache.getCellStores(target)
	for _, store := range r.cache.getStores() {
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

	newPeer, err := r.cache.allocPeer(bestStore.getID())
	if err != nil {
		log.Errorf("scheduler: allocate peer failure, errors:\n %+v", err)
		return nil, 0
	}
	return &newPeer, bestScore
}

// getDistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func getDistinctScore(cfg *Cfg, stores []*storeRuntimeInfo, other *storeRuntimeInfo) float64 {
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
func compareStoreScore(cfg *Cfg, storeA *storeRuntimeInfo, scoreA float64, storeB *storeRuntimeInfo, scoreB float64) int {
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
