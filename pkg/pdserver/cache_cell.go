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
	"bytes"

	"github.com/deepfabric/elasticell/pkg/storage/meta"
	"github.com/google/btree"
)

const (
	defaultBTreeDegree = 64
)

type cellItem struct {
	cell *meta.CellMeta
}

type cellTree struct {
	tree *btree.BTree
}

func newCellTree() *cellTree {
	return &cellTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Less returns true if the cell start key is greater than the other.
// So we will sort the cell with start key reversely.
func (r *cellItem) Less(other btree.Item) bool {
	left := r.cell.Min
	right := other.(*cellItem).cell.Min
	return bytes.Compare(left, right) > 0
}

func (r *cellItem) Contains(key []byte) bool {
	start, end := r.cell.Min, r.cell.Max
	// len(end) == 0: max field is positive infinity
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (t *cellTree) length() int {
	return t.tree.Len()
}

// update updates the tree with the cell.
// It finds and deletes all the overlapped cells first, and then
// insert the cell.
func (t *cellTree) update(cell *meta.CellMeta) {
	item := &cellItem{cell: cell}

	result := t.find(cell)
	if result == nil {
		result = item
	}

	var overlaps []*cellItem

	// between [cell, first], so is iterator all.min >= cell.min' cell
	// until all.min > cell.max
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*cellItem)
		// cell.max <= i.start, so cell and i has no overlaps,
		// otherwise cell and i has overlaps
		if len(cell.Max) > 0 && bytes.Compare(cell.Max, over.cell.Min) <= 0 {
			return false
		}
		overlaps = append(overlaps, over)
		return true
	})

	for _, item := range overlaps {
		t.tree.Delete(item)
	}

	t.tree.ReplaceOrInsert(item)
}

// remove removes a cell if the cell is in the tree.
// It will do nothing if it cannot find the cell or the found cell
// is not the same with the cell.
func (t *cellTree) remove(cell *meta.CellMeta) {
	result := t.find(cell)
	if result == nil || result.cell.ID != cell.ID {
		return
	}

	t.tree.Delete(result)
}

// search returns a cell that contains the key.
func (t *cellTree) search(key []byte) *meta.CellMeta {
	cell := &meta.CellMeta{Min: key}
	result := t.find(cell)
	if result == nil {
		return nil
	}
	return result.cell
}

func (t *cellTree) find(cell *meta.CellMeta) *cellItem {
	item := &cellItem{cell: cell}

	var result *cellItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*cellItem)
		return false
	})

	if result == nil || !result.Contains(cell.Min) {
		return nil
	}

	return result
}

type cellRuntime struct {
	cell   *meta.CellMeta
	leader *meta.CellMeta
	// downPeers    []*pdpb.PeerStats
	pendingPeers []*meta.PeerMeta
}

type cellCache struct {
	tree      *cellTree
	cells     map[uint64]*cellRuntime            // cellID -> cellRuntime
	leaders   map[uint64]map[uint64]*cellRuntime // storeID -> cellID -> cellRuntime
	followers map[uint64]map[uint64]*cellRuntime // storeID -> cellID -> cellRuntime
}

func newCellRuntime(cell *meta.CellMeta) *cellRuntime {
	return &cellRuntime{
		cell: cell,
	}
}

func newCellCache() *cellCache {
	cc := new(cellCache)
	cc.tree = newCellTree()
	cc.cells = make(map[uint64]*cellRuntime)
	cc.leaders = make(map[uint64]map[uint64]*cellRuntime)
	cc.followers = make(map[uint64]map[uint64]*cellRuntime)

	return cc
}

func (cc *cellRuntime) getID() uint64 {
	if cc.cell == nil {
		return 0
	}

	return cc.cell.ID
}

func (cc *cellRuntime) getPeers() []*meta.PeerMeta {
	if cc.cell == nil {
		return nil
	}

	return cc.cell.Peers
}

func (cc *cellCache) addCell(origin *cellRuntime) {
	if cell, ok := cc.cells[origin.getID()]; ok {
		cc.removeCell(cell)
	}

	// Add to tree and regions.
	cc.tree.update(origin.cell)
	cc.cells[origin.getID()] = origin

	if origin.leader == nil {
		return
	}

	// Add to leaders and followers.
	for _, peer := range origin.getPeers() {
		storeID := peer.StoreID
		if peer.ID == origin.leader.ID {
			// Add leader peer to leaders.
			store, ok := cc.leaders[storeID]
			if !ok {
				store = make(map[uint64]*cellRuntime)
				cc.leaders[storeID] = store
			}
			store[origin.getID()] = origin
		} else {
			// Add follower peer to followers.
			store, ok := cc.followers[storeID]
			if !ok {
				store = make(map[uint64]*cellRuntime)
				cc.followers[storeID] = store
			}
			store[origin.getID()] = origin
		}
	}
}

func (cc *cellCache) removeCell(origin *cellRuntime) {
	// Remove from tree and cells.
	cc.tree.remove(origin.cell)
	delete(cc.cells, origin.getID())

	// Remove from leaders and followers.
	for _, peer := range origin.getPeers() {
		delete(cc.leaders[peer.StoreID], origin.getID())
		delete(cc.followers[peer.StoreID], origin.getID())
	}
}
