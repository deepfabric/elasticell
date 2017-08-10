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

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

const (
	balanceLeaderSchedulerName = "balance-leader-scheduler"
)

type balanceLeaderScheduler struct {
	cfg      *Cfg
	limit    uint64
	selector Selector
}

func newBalanceLeaderScheduler(cfg *Cfg) *balanceLeaderScheduler {
	var filters []Filter
	filters = append(filters, newBlockFilter())
	filters = append(filters, newStateFilter(cfg))
	filters = append(filters, newHealthFilter(cfg))

	return &balanceLeaderScheduler{
		cfg:      cfg,
		limit:    1,
		selector: newBalanceSelector(leaderKind, filters),
	}
}

func (l *balanceLeaderScheduler) GetName() string {
	return balanceLeaderSchedulerName
}

func (l *balanceLeaderScheduler) GetResourceKind() ResourceKind {
	return leaderKind
}

func (l *balanceLeaderScheduler) GetResourceLimit() uint64 {
	return minUint64(l.limit, l.cfg.Schedule.LeaderScheduleLimit)
}

func (l *balanceLeaderScheduler) Prepare(cache *cache) error { return nil }

func (l *balanceLeaderScheduler) Cleanup(cache *cache) {}

func (l *balanceLeaderScheduler) Schedule(cache *cache) Operator {
	cell, newLeader := scheduleTransferLeader(cache, l.selector)
	if cell == nil {
		return nil
	}

	source := cache.getStoreCache().getStore(cell.LeaderPeer.StoreID)
	target := cache.getStoreCache().getStore(newLeader.StoreID)
	if !shouldBalance(source, target, l.GetResourceKind()) {
		return nil
	}
	l.limit = adjustBalanceLimit(cache, l.GetResourceKind())

	return newTransferLeaderAggregationOp(cell, newLeader)
}

// scheduleTransferLeader schedules a cell to transfer leader to the peer.
func scheduleTransferLeader(cache *cache, s Selector, filters ...Filter) (*CellInfo, *metapb.Peer) {
	stores := cache.getStoreCache().getStores()
	if len(stores) == 0 {
		return nil, nil
	}

	var averageLeader float64
	for _, s := range stores {
		averageLeader += float64(s.leaderScore()) / float64(len(stores))
	}

	mostLeaderStore := s.SelectSource(stores, filters...)
	leastLeaderStore := s.SelectTarget(stores, filters...)

	var mostLeaderDistance, leastLeaderDistance float64
	if mostLeaderStore != nil {
		mostLeaderDistance = math.Abs(mostLeaderStore.leaderScore() - averageLeader)
	}
	if leastLeaderStore != nil {
		leastLeaderDistance = math.Abs(leastLeaderStore.leaderScore() - averageLeader)
	}
	if mostLeaderDistance == 0 && leastLeaderDistance == 0 {
		return nil, nil
	}

	if mostLeaderDistance > leastLeaderDistance {
		// Transfer a leader out of mostLeaderStore.
		cell := cache.getCellCache().randLeaderCell(mostLeaderStore.getID())
		if cell == nil {
			return nil, nil
		}
		targetStores := cache.getStoreCache().getFollowerStores(cell)
		target := s.SelectTarget(targetStores)
		if target == nil {
			return nil, nil
		}

		return cell, cell.getStorePeer(target.getID())
	}

	// Transfer a leader into leastLeaderStore.
	cell := cache.getCellCache().randFollowerCell(leastLeaderStore.getID())
	if cell == nil {
		return nil, nil
	}
	return cell, cell.getStorePeer(leastLeaderStore.getID())
}
