/*
The priority queue is rewrite of github.com/Workiva/go-datastructures/queue/priority_queue.go.
- removed lock protection
- removed de-duplication support
- removed dispose support
- removed error return of Put and Get
- removed block behavior of (*PriorityQueue).Get()
- replaced home-brewed heap impl with the one inside std library
*/

package util

import (
	"container/heap"
	"sort"
)

// Item is an item that can be added to the priority queue.
type Item interface {
	// Compare returns a int that can be used to determine
	// ordering in the priority queue.  Assuming the queue
	// is in ascending order, this should return > logic.
	// Return 1 to indicate this object is greater than the
	// the other logic, 0 to indicate equality, and -1 to indicate
	// less than other.
	Compare(other Item) int
}

type priorityItems []Item

// Len is part of sort.Interface.
func (s priorityItems) Len() int {
	return len(s)
}

// Swap is part of sort.Interface.
func (s priorityItems) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less is part of sort.Interface.
func (s priorityItems) Less(i, j int) bool {
	return s[i].Compare(s[j]) < 0
}

// Push is part of heap.Interface.
func (s *priorityItems) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*s = append(*s, x.(Item))
}

// Pop is part of heap.Interface.
func (s *priorityItems) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[0 : n-1]
	return x
}

// PriorityQueue is similar to queue except that it takes
// items that implement the Item interface and adds them
// to the queue in priority order.
type PriorityQueue struct {
	items   priorityItems
	capHint int //capacity hint of items
}

// Put adds items to the queue.
func (pq *PriorityQueue) Put(items ...Item) {
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		heap.Push(&pq.items, item)
	}
	return
}

// Get retrieves an item from the queue if non-empty, othewise returns nil
func (pq *PriorityQueue) Get() (item Item) {
	if len(pq.items) == 0 {
		return
	}
	item = heap.Pop(&pq.items).(Item)
	return
}

// BulkGet retrieves items from the queue.
// len(items) will be max(0, min(number, len(pq.items)))
// items is sorted in ascending order.
func (pq *PriorityQueue) BulkGet(number int) (items []Item) {
	if number < 1 {
		items = make([]Item, 0)
		return
	}
	if number >= len(pq.items) {
		sort.Sort(pq.items)
		items = pq.items
		pq.items = make(priorityItems, 0, pq.capHint)
		return
	}
	items = make([]Item, number, number)
	for i := 0; i < number; i++ {
		items[i] = heap.Pop(&pq.items).(Item)
	}
	return
}

// Peek will look at the next item without removing it from the queue.
func (pq *PriorityQueue) Peek() Item {
	if len(pq.items) > 0 {
		return pq.items[0]
	}
	return nil
}

// Empty returns a bool indicating if there are any items left
// in the queue.
func (pq *PriorityQueue) Empty() bool {
	return len(pq.items) == 0
}

// Len returns a number indicating how many items are in the queue.
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// NewPriorityQueue is the constructor for a priority queue.
func NewPriorityQueue(capHint int) *PriorityQueue {
	return &PriorityQueue{
		items:   make(priorityItems, 0, capHint),
		capHint: capHint,
	}
}
