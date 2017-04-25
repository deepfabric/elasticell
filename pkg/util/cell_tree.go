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

package util

import (
	"bytes"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/google/btree"
)

const (
	defaultBTreeDegree = 64
)

// CellItem is the cell btree item
type CellItem struct {
	cell metapb.Cell
}

// CellTree is the btree for cell
type CellTree struct {
	tree *btree.BTree
}

// NewCellTree returns a default cell btree
func NewCellTree() *CellTree {
	return &CellTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

// Less returns true if the cell start key is greater than the other.
// So we will sort the cell with start key reversely.
func (r *CellItem) Less(other btree.Item) bool {
	left := r.cell.Start
	right := other.(*CellItem).cell.Start
	return bytes.Compare(left, right) > 0
}

// Contains returns the item contains the key
func (r *CellItem) Contains(key []byte) bool {
	start, end := r.cell.Start, r.cell.End
	// len(end) == 0: max field is positive infinity
	return bytes.Compare(key, start) >= 0 && (len(end) == 0 || bytes.Compare(key, end) < 0)
}

func (t *CellTree) length() int {
	return t.tree.Len()
}

// Update updates the tree with the cell.
// It finds and deletes all the overlapped cells first, and then
// insert the cell.
func (t *CellTree) Update(cell metapb.Cell) {
	item := &CellItem{cell: cell}

	result := t.find(cell)
	if result == nil {
		result = item
	}

	var overlaps []*CellItem

	// between [cell, first], so is iterator all.min >= cell.min' cell
	// until all.min > cell.max
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*CellItem)
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

// Remove removes a cell if the cell is in the tree.
// It will do nothing if it cannot find the cell or the found cell
// is not the same with the cell.
func (t *CellTree) Remove(cell metapb.Cell) bool {
	result := t.find(cell)
	if result == nil || result.cell.ID != cell.ID {
		return false
	}

	t.tree.Delete(result)
	return true
}

// Ascend asc iterator the tree until fn returns false
func (t *CellTree) Ascend(fn func(cell *metapb.Cell) bool) {
	t.tree.Descend(func(item btree.Item) bool {
		return fn(&item.(*CellItem).cell)
	})
}

// NextCell return the next bigger key range cell
func (t *CellTree) NextCell(start []byte) *metapb.Cell {
	var value *CellItem

	p := &CellItem{
		cell: metapb.Cell{Start: start},
	}

	t.tree.DescendLessOrEqual(p, func(item btree.Item) bool {
		if bytes.Compare(item.(*CellItem).cell.Start, start) > 0 {
			value = item.(*CellItem)
			return false
		}

		return true
	})

	if nil == value {
		return nil
	}

	return &value.cell
}

// AscendRange asc iterator the tree in the range [start, end) until fn returns false
func (t *CellTree) AscendRange(start, end []byte, fn func(cell *metapb.Cell) bool) {
	startItem := &CellItem{
		cell: metapb.Cell{Start: start},
	}

	endItem := &CellItem{
		cell: metapb.Cell{Start: end},
	}

	t.tree.DescendRange(startItem, endItem, func(item btree.Item) bool {
		return fn(&item.(*CellItem).cell)
	})
}

// Search returns a cell that contains the key.
func (t *CellTree) Search(key []byte) metapb.Cell {
	cell := metapb.Cell{Start: key}
	result := t.find(cell)
	if result == nil {
		return metapb.Cell{}
	}
	return result.cell
}

func (t *CellTree) find(cell metapb.Cell) *CellItem {
	item := &CellItem{cell: cell}

	var result *CellItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*CellItem)
		return false
	})

	if result == nil || !result.Contains(cell.Start) {
		return nil
	}

	return result
}
