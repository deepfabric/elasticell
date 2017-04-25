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
	"testing"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

func TestTree(t *testing.T) {
	tree := NewCellTree()

	tree.Update(metapb.Cell{
		ID:    1,
		Start: []byte{0},
		End:   []byte{1},
	})

	tree.Update(metapb.Cell{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})

	tree.Update(metapb.Cell{
		ID:    3,
		Start: []byte{4},
		End:   []byte{5},
	})

	if tree.tree.Len() != 3 {
		t.Errorf("tree failed, insert 3 elements, but only %d", tree.tree.Len())
	}

	expect := []byte{0, 2, 4}
	count := 0
	tree.Ascend(func(cell *metapb.Cell) bool {
		if expect[count] != cell.Start[0] {
			t.Error("tree failed, asc order is error")
		}
		count++

		return true
	})

	cell := tree.Search([]byte{2})
	if len(cell.Start) == 0 || cell.Start[0] != 2 {
		t.Error("tree failed, search failed")
	}

	c := tree.NextCell(nil)
	if c == nil || len(c.Start) == 0 || c.Start[0] != 0 {
		t.Error("tree failed, search next failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{4}, func(cell *metapb.Cell) bool {
		count++
		return true
	})

	if count != 2 {
		t.Error("tree failed, asc range failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{5}, func(cell *metapb.Cell) bool {
		count++
		return true
	})

	if count != 3 {
		t.Error("tree failed, asc range failed")
	}

	// it will replace with 0,1 cell
	tree.Update(metapb.Cell{
		ID:    10,
		Start: nil,
		End:   []byte{1},
	})
	cell = tree.Search([]byte{0})
	if len(cell.Start) != 0 && cell.Start[0] == 0 {
		t.Error("tree failed, update overlaps failed")
	}

	tree.Remove(metapb.Cell{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})
	if tree.length() != 2 {
		t.Error("tree failed, Remove failed")
	}
}
