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

	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

// changePeerOperator is sub operator of cellOperator
type changePeerOperator struct {
	Name       string          `json:"name"`
	CellID     uint64          `json:"cellID"`
	ChangePeer pdpb.ChangePeer `json:"changePeer"`
}

func (op *changePeerOperator) String() string {
	return fmt.Sprintf("%+v", *op)
}

func (op *changePeerOperator) GetCellID() uint64 {
	return op.CellID
}

func (op *changePeerOperator) GetResourceKind() ResourceKind {
	return cellKind
}

func (op *changePeerOperator) Do(cell *CellInfo) (*pdpb.CellHeartbeatRsp, bool) {
	// Check if operator is finished.
	peer := op.ChangePeer.Peer

	switch op.ChangePeer.Type {
	case pdpb.AddNode:
		if cell.getPendingPeer(peer.ID) != nil {
			// Peer is added but not finished.
			return nil, false
		}
		if cell.getPeer(peer.ID) != nil {
			// Peer is added and finished.
			return nil, true
		}
	case pdpb.RemoveNode:
		if cell.getPeer(peer.ID) == nil {
			// Peer is removed.
			return nil, true
		}
	}

	res := &pdpb.CellHeartbeatRsp{
		ChangePeer: &op.ChangePeer,
	}
	return res, false
}
