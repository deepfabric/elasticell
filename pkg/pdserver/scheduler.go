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
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

const (
	maxScheduleRetries  = 10
	maxScheduleInterval = time.Minute
	minScheduleInterval = time.Millisecond * 10
)

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	adminKind ResourceKind = iota
	leaderKind
	cellKind
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	GetName() string
	GetResourceKind() ResourceKind
	GetResourceLimit() uint64
	Prepare(cache *cache) error
	Cleanup(cache *cache)
	Schedule(cache *cache) Operator
}

type scheduleController struct {
	sync.Mutex

	Scheduler
	cfg      *Cfg
	limiter  *scheduleLimiter
	interval time.Duration
}

func newScheduleController(c *coordinator, s Scheduler) *scheduleController {
	return &scheduleController{
		Scheduler: s,
		cfg:       c.cfg,
		limiter:   c.limiter,
		interval:  minScheduleInterval,
	}
}

func (s *scheduleController) Schedule(cache *cache) Operator {
	// If we have schedule, reset interval to the minimal interval.
	if op := s.Scheduler.Schedule(cache); op != nil {
		s.interval = minScheduleInterval
		return op
	}

	// If we have no schedule, increase the interval exponentially.
	s.interval = minDuration(s.interval*2, maxScheduleInterval)
	return nil
}

func (s *scheduleController) GetInterval() time.Duration {
	return s.interval
}

func (s *scheduleController) AllowSchedule() bool {
	return s.limiter.operatorCount(s.GetResourceKind()) < s.GetResourceLimit()
}

type scheduleLimiter struct {
	sync.RWMutex
	counts map[ResourceKind]uint64
}

func newScheduleLimiter() *scheduleLimiter {
	return &scheduleLimiter{
		counts: make(map[ResourceKind]uint64),
	}
}

func (l *scheduleLimiter) addOperator(op Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]++
}

func (l *scheduleLimiter) removeOperator(op Operator) {
	l.Lock()
	defer l.Unlock()
	l.counts[op.GetResourceKind()]--
}

func (l *scheduleLimiter) operatorCount(kind ResourceKind) uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.counts[kind]
}

// scheduleRemovePeer schedules a cell to remove the peer.
func scheduleRemovePeer(cache *cache, s Selector, filters ...Filter) (*CellInfo, *metapb.Peer) {
	stores := cache.getStoreCache().getStores()

	source := s.SelectSource(stores, filters...)
	if source == nil {
		return nil, nil
	}

	cell := cache.getCellCache().randFollowerCell(source.getID())
	if cell == nil {
		cell = cache.getCellCache().randLeaderCell(source.getID())
	}
	if cell == nil {
		return nil, nil
	}

	return cell, cell.getStorePeer(source.getID())
}
