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
	"fmt"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

const (
	maxOperatorWaitTime = 5 * time.Minute
)

// aggregationOperator is the aggregations all operator on the same cell
type aggregationOperator struct {
	CellID    uint64     `json:"cellID"`
	StartAt   time.Time  `json:"startAt"`
	EndAt     time.Time  `json:"endAt"`
	LastIndex int        `json:"lastIndex"`
	Ops       []Operator `json:"ops"`
}

func (op *aggregationOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *aggregationOperator) GetCellID() uint64 {
	return op.CellID
}

func (op *aggregationOperator) GetResourceKind() ResourceKind {
	return op.Ops[0].GetResourceKind()
}

func (op *aggregationOperator) Do(target *CellInfo) (*pdpb.CellHeartbeatRsp, bool) {
	if time.Since(op.StartAt) > maxOperatorWaitTime {
		log.Errorf("scheduler: operator timeout, operator=<%s>", op)
		return nil, true
	}

	// If an operator is not finished, do it.
	for ; op.LastIndex < len(op.Ops); op.LastIndex++ {
		if res, finished := op.Ops[op.LastIndex].Do(target); !finished {
			return res, false
		}
	}

	op.EndAt = time.Now()
	return nil, true
}
