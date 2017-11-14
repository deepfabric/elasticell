/*
Ordered array is to solve "get M smallest ones among N(N>M) items".
This target problem is different with priority queue.
Ordered array and priority queue are both implemented with heap data-structure.
*/

package util

import (
	"container/heap"
	"sort"

	"github.com/pkg/errors"
)

// https://stackoverflow.com/questions/6878590/the-maximum-value-for-an-int-type-in-go
const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

// orderedItems is a max-heap
type orderedItems []Item

// Len is part of sort.Interface.
func (s orderedItems) Len() int {
	return len(s)
}

// Swap is part of sort.Interface.
func (s orderedItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less is part of sort.Interface.
func (s orderedItems) Less(i, j int) bool {
	return s[i].Compare(s[j]) > 0
}

// Push is part of heap.Interface.
func (s *orderedItems) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*s = append(*s, x.(Item))
}

// Pop is part of heap.Interface.
func (s *orderedItems) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[0 : n-1]
	return x
}

// OrderedArray is similar to size limited array except that it's to
// solve "get M smallest ones among N(N>M) items".
type OrderedArray struct {
	items    orderedItems
	capacity int //size limit of items
}

// Put adds items to the queue.
func (oa *OrderedArray) Put(items ...Item) {
	for _, item := range items {
		if len(oa.items) >= oa.capacity {
			if oa.items[0].Compare(item) > 0 {
				oa.items[0] = item
				heap.Fix(&oa.items, 0)
			}
		} else {
			heap.Push(&oa.items, item)
		}
	}
	return
}

// Finalize retrieves all items from and clear the array.
// items is sorted in ascending order.
func (oa *OrderedArray) Finalize() (items []Item) {
	sort.Sort(sort.Reverse(oa.items))
	items = oa.items
	oa.items = make(orderedItems, 0, oa.capacity)
	return
}

// Len returns a number indicating how many items are in the array.
func (oa *OrderedArray) Len() int {
	return len(oa.items)
}

// NewOrderedArray is the constructor for an ordered array.
// capacity is size limit of queue. capacity shall > 0.
func NewOrderedArray(capacity int) (oa *OrderedArray, err error) {
	if capacity <= 0 {
		err = errors.Errorf("invalid parameter capacity, have %v, want >0", capacity)
		return
	}
	oa = &OrderedArray{
		items:    make(orderedItems, 0, capacity),
		capacity: capacity,
	}
	return
}
