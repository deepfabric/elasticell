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

package pb

import (
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
)

// BaseReq is for abstract
type BaseReq interface {
	GetFrom() string
	GetId() uint64
}

// NewCell returns a cell meta
func NewCell(cellID, peerID, storeID uint64) meta.Cell {
	c := meta.Cell{
		ID:    cellID,
		Epoch: newCellEpoch(),
	}

	c.Peers = append(c.Peers, &meta.Peer{
		ID:      peerID,
		StoreID: storeID,
	})

	return c
}

func newCellEpoch() meta.CellEpoch {
	return meta.CellEpoch{
		ConfVer: 1,
		CellVer: 1,
	}
}
