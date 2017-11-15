package datastructures

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderedArrayNew(t *testing.T) {
	q1 := NewOrderedArray(-1)
	q2 := NewOrderedArray(0)

	for i := 0; i < 10000; i++ {
		q1.Put(mockItem(i))
		q2.Put(mockItem(i))
	}
	assert.Equal(t, 10000, q1.Len())
	assert.Equal(t, 10000, q2.Len())
}

func TestOrderedArrayPut(t *testing.T) {
	q := NewOrderedArray(10)

	q.Put(mockItem(2))

	assert.Len(t, q.items, 1)
	assert.Equal(t, mockItem(2), q.items[0])

	q.Put(mockItem(1))

	if !assert.Len(t, q.items, 2) {
		return
	}
	assert.Equal(t, mockItem(1), q.items[1])
	assert.Equal(t, mockItem(2), q.items[0])
}

func TestOrderedArrayMerge(t *testing.T) {
	q1 := NewOrderedArray(1000)
	q2 := NewOrderedArray(1000)
	for i := 0; i < 250; i++ {
		q1.Put(mockItem(i))
	}
	for i := 750; i < 10000; i++ {
		q1.Put(mockItem(i))
	}
	for i := 250; i < 750; i++ {
		q2.Put(mockItem(i))
	}
	for i := 1000; i < 2000; i++ {
		q2.Put(mockItem(i))
	}
	q1.Merge(q2)
	assert.Equal(t, 1000, q1.Len())

	result := q1.Finalize()
	assert.Equal(t, 1000, len(result))

	for i := 0; i < len(result); i++ {
		assert.Equal(t, mockItem(i), result[i])
	}
}

func TestOrderedArrayFinalize(t *testing.T) {
	q := NewOrderedArray(1000)
	for i := 0; i < 500; i++ {
		q.Put(mockItem(i))
	}
	for i := 1000; i < 10000; i++ {
		q.Put(mockItem(i))
	}
	for i := 500; i < 1000; i++ {
		q.Put(mockItem(i))
	}
	assert.Equal(t, 1000, q.Len())

	result := q.Finalize()
	assert.Equal(t, 0, q.Len())
	assert.Equal(t, 1000, len(result))

	for i := 0; i < len(result); i++ {
		assert.Equal(t, mockItem(i), result[i])
	}
}

func BenchmarkOrderedPutFinalize(b *testing.B) {
	capHint := 1000
	q := NewOrderedArray(capHint)
	for i := 0; i < b.N; i++ {
		q.Put(mockItem(i))
	}
	_ = q.Finalize()
}
