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

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var (
	emptyRsp = &pdpb.CellHeartbeatRsp{}
)

type coordinator struct {
	sync.RWMutex
	storeLock sync.RWMutex

	cfg        *Cfg
	cache      *cache
	checker    *replicaChecker
	limiter    *scheduleLimiter
	opts       map[uint64]Operator
	storeOpts  map[uint64]StoreOperator
	schedulers map[string]*scheduleController
	runner     *util.Runner
}

func newCoordinator(cfg *Cfg, cache *cache) *coordinator {
	c := new(coordinator)
	c.cfg = cfg
	c.cache = cache
	c.checker = newReplicaChecker(cfg, cache)
	c.limiter = newScheduleLimiter()
	c.opts = make(map[uint64]Operator)
	c.storeOpts = make(map[uint64]StoreOperator)
	c.schedulers = make(map[string]*scheduleController)
	c.runner = util.NewRunner()

	return c
}

func (c *coordinator) run() {
	c.addScheduler(newBalanceLeaderScheduler(c.cfg))
	c.addScheduler(newBalanceCellScheduler(c.cfg))
}

func (c *coordinator) stop() {
	c.runner.Stop()
}

func (c *coordinator) dispatchStore(target *StoreInfo) *pdpb.StoreHeartbeatRsp {
	if op := c.getStoreOperator(target.Meta.ID); op != nil {
		res, finished := op.Do(target)
		if !finished {
			return res
		}
		c.removeStoreOperator(op)
	}

	return &pdpb.StoreHeartbeatRsp{}
}

// dispatch is used for coordinator cell,
// it will coordinator when the heartbeat arrives
func (c *coordinator) dispatch(target *CellInfo) *pdpb.CellHeartbeatRsp {
	// Check existed operator.
	if op := c.getOperator(target.Meta.ID); op != nil {
		res, finished := op.Do(target)
		if !finished {
			return res
		}
		c.removeOperator(op)
	}

	// Check replica operator.
	if c.limiter.operatorCount(cellKind) >= c.cfg.Schedule.ReplicaScheduleLimit {
		return nil
	}

	if op := c.checker.Check(target); op != nil {
		if c.addOperator(op) {
			res, _ := op.Do(target)
			return res
		}
	}

	return nil
}

func (c *coordinator) getOperators() []interface{} {
	c.RLock()
	defer c.RUnlock()

	var opts []interface{}
	for _, op := range c.opts {
		opts = append(opts, op)
	}

	return opts
}

func (c *coordinator) getOperator(cellID uint64) Operator {
	c.RLock()
	defer c.RUnlock()

	return c.opts[cellID]
}

func (c *coordinator) getStoreOperator(storeID uint64) StoreOperator {
	c.storeLock.RLock()
	defer c.storeLock.RUnlock()

	return c.storeOpts[storeID]
}

func (c *coordinator) getOperatorCount() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.opts)
}

func (c *coordinator) addOperator(op Operator) bool {
	c.Lock()
	defer c.Unlock()

	cellID := op.GetCellID()

	if _, ok := c.opts[cellID]; ok {
		return false
	}

	c.limiter.addOperator(op)
	c.opts[cellID] = op
	return true
}

func (c *coordinator) addStoreOperator(op StoreOperator) bool {
	c.storeLock.Lock()
	defer c.storeLock.Unlock()

	storeID := op.GetStoreID()

	if _, ok := c.storeOpts[storeID]; ok {
		return false
	}

	c.storeOpts[storeID] = op
	return true
}

func (c *coordinator) removeOperator(op Operator) {
	c.Lock()
	id := op.GetCellID()
	c.limiter.removeOperator(op)
	delete(c.opts, id)
	c.Unlock()
}

func (c *coordinator) removeStoreOperator(op StoreOperator) {
	c.storeLock.Lock()
	delete(c.storeOpts, op.GetStoreID())
	c.storeLock.Unlock()
}

func (c *coordinator) getScheduler(name string) *scheduleController {
	c.RLock()
	defer c.RUnlock()

	return c.schedulers[name]
}

func (c *coordinator) addScheduler(scheduler Scheduler) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cache); err != nil {
		return errors.Wrapf(err, "")
	}

	c.runner.RunCancelableTask(func(ctx context.Context) {
		c.runScheduler(ctx, s)
	})

	c.schedulers[s.GetName()] = s
	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.schedulers[name]
	if !ok {
		return errSchedulerNotFound
	}

	delete(c.schedulers, name)
	return nil
}

func (c *coordinator) runScheduler(ctx context.Context, s *scheduleController) {
	defer s.Cleanup(c.cache)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("coordinator: scheduler stopped: scheduler=<%s>", s.GetName())
			return
		case <-timer.C:
			timer.Reset(s.GetInterval())

			s.Lock()
			if !s.AllowSchedule() {
				s.Unlock()
				continue
			}

			for i := 0; i < maxScheduleRetries; i++ {
				op := s.Schedule(c.cache)
				if op == nil {
					continue
				}
				if c.addOperator(op) {
					break
				}
			}
			s.Unlock()
		}
	}
}
