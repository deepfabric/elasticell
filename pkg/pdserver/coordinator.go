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

	pb "github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"golang.org/x/net/context"
)

var (
	emptyRsp = &pb.CellHeartbeatRsp{}
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	checker *replicaChecker
	opts    map[uint64]Operator
}

func newCoordinator(cfg *Cfg, cache *cache) *coordinator {
	c := new(coordinator)
	c.opts = make(map[uint64]Operator)
	c.checker = newReplicaChecker(cfg, cache)
	c.ctx, c.cancel = context.WithCancel(context.Background())

	return c
}

// dispatch is used for coordinator cell,
// it will coordinator when the heartbeat arrives
func (c *coordinator) dispatch(target *cellRuntime) *pb.CellHeartbeatRsp {
	// Check existed operator.
	// if op := c.getOperator(region.GetId()); op != nil {
	// 	res, finished := op.Do(region)
	// 	if !finished {
	// 		return res
	// 	}
	// 	c.removeOperator(op)
	// }

	// Check replica operator.
	// if c.limiter.operatorCount(regionKind) >= c.opt.GetReplicaScheduleLimit() {
	// 	return nil
	// }

	if op := c.checker.Check(target); op != nil {
		if c.addOperator(op) {
			res, _ := op.Do(target)
			return res
		}
	}

	return nil
}

func (c *coordinator) addOperator(op Operator) bool {
	c.Lock()
	defer c.Unlock()

	cellID := op.GetCellID()

	if _, ok := c.opts[cellID]; ok {
		return false
	}

	// TODO: limiter
	// c.limiter.addOperator(op)
	c.opts[cellID] = op
	return true
}
