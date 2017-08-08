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

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

type transferLeaderOperator struct {
	Name      string       `json:"name"`
	CellID    uint64       `json:"cellId"`
	OldLeader *metapb.Peer `json:"oldLeader"`
	NewLeader *metapb.Peer `json:"newLeader"`
}

func newTransferLeaderOperator(cellID uint64, oldLeader, newLeader *metapb.Peer) *transferLeaderOperator {
	return &transferLeaderOperator{
		Name:      "transfer_leader",
		CellID:    cellID,
		OldLeader: oldLeader,
		NewLeader: newLeader,
	}
}

func (op *transferLeaderOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *transferLeaderOperator) GetCellID() uint64 {
	return op.CellID
}

func (op *transferLeaderOperator) GetResourceKind() ResourceKind {
	return leaderKind
}

func (op *transferLeaderOperator) Do(cell *CellInfo) (*pdpb.CellHeartbeatRsp, bool) {
	// Check if operator is finished.
	if cell.LeaderPeer.ID == op.NewLeader.ID {
		return nil, true
	}

	res := &pdpb.CellHeartbeatRsp{
		TransferLeader: &pdpb.TransferLeader{
			Peer: *op.NewLeader,
		},
	}
	return res, false
}
