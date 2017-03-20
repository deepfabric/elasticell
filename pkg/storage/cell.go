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

package storage

import (
	"sync"

	"github.com/deepfabric/elasticell/pkg/storage/meta"
)

// Cell cell is a set of continuous key, it's the base unit of data relocation.
// The first cell is created by pd server when cluster is first bootstrap.
// Cell has features as below:
// 1. Splite, when a cell is too large.
// 2. Merge, when some cells is too small.
// 3. Replication of cells compose a raft group.
type Cell struct {
	mux  sync.RWMutex
	meta *meta.CellMeta
}

// NewCell create a empty cell, used for create a first cell
func NewCell(id, storeID uint64) *Cell {
	return &Cell{
		meta: meta.NewCellMeta(id, storeID),
	}
}

// GetCellMeta returns meta data of cell
func (c *Cell) GetCellMeta() ([]byte, error) {
	c.mux.RLock()
	data, err := c.meta.Marshal()
	c.mux.RUnlock()

	return data, err
}

// GetID returns cell id
func (c *Cell) GetID() uint64 {
	return c.meta.ID
}
