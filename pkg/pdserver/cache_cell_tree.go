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

	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/google/btree"
)

const (
	defaultBTreeDegree = 64
)

type cellItem struct {
	cell meta.Cell
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
	left := r.cell.Start
	right := other.(*cellItem).cell.Start
	return bytes.Compare(left, right) > 0
}

func (r *cellItem) Contains(key []byte) bool {
	start, end := r.cell.Start, r.cell.End
	// len(end) == 0: max field is positive infinity
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (t *cellTree) length() int {
	return t.tree.Len()
}

// update updates the tree with the cell.
// It finds and deletes all the overlapped cells first, and then
// insert the cell.
func (t *cellTree) update(cell meta.Cell) {
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
		if len(cell.End) > 0 && bytes.Compare(cell.End, over.cell.Start) <= 0 {
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
func (t *cellTree) remove(cell meta.Cell) {
	result := t.find(cell)
	if result == nil || result.cell.ID != cell.ID {
		return
	}

	t.tree.Delete(result)
}

// search returns a cell that contains the key.
func (t *cellTree) search(key []byte) meta.Cell {
	cell := meta.Cell{Start: key}
	result := t.find(cell)
	if result == nil {
		return meta.Cell{}
	}
	return result.cell
}

func (t *cellTree) find(cell meta.Cell) *cellItem {
	item := &cellItem{cell: cell}

	var result *cellItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*cellItem)
		return false
	})

	if result == nil || !result.Contains(cell.Start) {
		return nil
	}

	return result
}
