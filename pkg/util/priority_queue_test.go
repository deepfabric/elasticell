/*
The priority queue is rewrite of github.com/Workiva/go-datastructures/queue/priority_queue.go.
*/

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockItem int

func (mi mockItem) Compare(other Item) int {
	omi := other.(mockItem)
	if mi > omi {
		return 1
	} else if mi == omi {
		return 0
	}
	return -1
}

func TestPriorityPut(t *testing.T) {
	q := NewPriorityQueue(1)

	q.Put(mockItem(2))

	assert.Len(t, q.items, 1)
	assert.Equal(t, mockItem(2), q.items[0])

	q.Put(mockItem(1))

	if !assert.Len(t, q.items, 2) {
		return
	}
	assert.Equal(t, mockItem(1), q.items[0])
	assert.Equal(t, mockItem(2), q.items[1])
}

func TestPriorityGet(t *testing.T) {
	q := NewPriorityQueue(1)

	q.Put(mockItem(2))
	result := q.BulkGet(2)

	if !assert.Len(t, result, 1) {
		return
	}

	assert.Equal(t, mockItem(2), result[0])
	assert.Len(t, q.items, 0)

	q.Put(mockItem(2))
	q.Put(mockItem(1))

	result = q.BulkGet(1)
	if !assert.Len(t, result, 1) {
		return
	}

	assert.Equal(t, mockItem(1), result[0])
	assert.Len(t, q.items, 1)

	result = q.BulkGet(2)
	if !assert.Len(t, result, 1) {
		return
	}

	assert.Equal(t, mockItem(2), result[0])
}

func TestAddEmptyPriorityPut(t *testing.T) {
	q := NewPriorityQueue(1)

	q.Put()

	assert.Len(t, q.items, 0)
}

func TestPriorityGetNonPositiveNumber(t *testing.T) {
	q := NewPriorityQueue(1)

	q.Put(mockItem(1))

	result := q.BulkGet(0)

	assert.Len(t, result, 0)

	result = q.BulkGet(-1)
	assert.Len(t, result, 0)
}

func TestPriorityEmpty(t *testing.T) {
	q := NewPriorityQueue(1)
	assert.True(t, q.Empty())

	q.Put(mockItem(1))

	assert.False(t, q.Empty())
}

func TestPriorityGetEmpty(t *testing.T) {
	q := NewPriorityQueue(1)

	result := q.Get()
	if !assert.Equal(t, result, nil) {
		return
	}
}

func TestPriorityPeek(t *testing.T) {
	q := NewPriorityQueue(1)
	q.Put(mockItem(1))

	assert.Equal(t, mockItem(1), q.Peek())

	result := q.Get()
	assert.Equal(t, result, mockItem(1))
}

func BenchmarkPriorityQueuePutGet(b *testing.B) {
	capHint := 1000
	q := NewPriorityQueue(capHint)
	for i := 0; i < b.N; i++ {
		q.Put(mockItem(i))
	}
	for i := 0; i < b.N; i++ {
		q.Get()
	}
}
