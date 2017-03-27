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

	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

// Operator is an interface to scheduler cell
type Operator interface {
	GetCellID() uint64
	Do(cell *cellRuntime) (*pdpb.CellHeartbeatRsp, bool)
}

func newAddPeerAggregationOp(cell *cellRuntime, peer meta.Peer) Operator {
	addPeerOp := newAddPeerOp(cell.getID(), peer)
	return newAggregationOp(cell, addPeerOp)
}

func newAddPeerOp(cellID uint64, peer meta.Peer) *changePeerOperator {
	return &changePeerOperator{
		CellID: cellID,
		ChangePeer: pdpb.ChangePeer{
			Type: pdpb.AddNode,
			Peer: &peer,
		},
	}
}

func newAggregationOp(cell *cellRuntime, ops ...Operator) *aggregationOperator {
	if len(ops) == 0 {
		log.Fatal("scheduler: create new cell aggregation operator use empty opts")
	}

	return &aggregationOperator{
		CellID:  cell.getID(),
		StartAt: time.Now(),
		Ops:     ops,
	}
}
