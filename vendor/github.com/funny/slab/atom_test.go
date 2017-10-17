package slab

import (
	"testing"

	"github.com/funny/utest"
)

func Test_AtomPool_AllocAndFree(t *testing.T) {
	pool := NewAtomPool(128, 64*1024, 2, 1024*1024)
	for i := 0; i < len(pool.classes); i++ {
		temp := make([][]byte, len(pool.classes[i].chunks))

		for j := 0; j < len(temp); j++ {
			mem := pool.Alloc(pool.classes[i].size)
			utest.EqualNow(t, cap(mem), pool.classes[i].size)
			temp[j] = mem
		}
		utest.Assert(t, pool.classes[i].head == 0)

		for j := 0; j < len(temp); j++ {
			pool.Free(temp[j])
		}
		utest.Assert(t, pool.classes[i].head != 0)
	}
}

func Test_AtomPool_AllocSmall(t *testing.T) {
	pool := NewAtomPool(128, 1024, 2, 1024)
	mem := pool.Alloc(64)
	utest.EqualNow(t, len(mem), 64)
	utest.EqualNow(t, cap(mem), 128)
	pool.Free(mem)
}

func Test_AtomPool_AllocLarge(t *testing.T) {
	pool := NewAtomPool(128, 1024, 2, 1024)
	mem := pool.Alloc(2048)
	utest.EqualNow(t, len(mem), 2048)
	utest.EqualNow(t, cap(mem), 2048)
	pool.Free(mem)
}

func Test_AtomPool_DoubleFree(t *testing.T) {
	pool := NewAtomPool(128, 1024, 2, 1024)
	mem := pool.Alloc(64)
	go func() {
		defer func() {
			utest.NotNilNow(t, recover())
		}()
		pool.Free(mem)
		pool.Free(mem)
	}()
}

func Test_AtomPool_AllocSlow(t *testing.T) {
	pool := NewAtomPool(128, 1024, 2, 1024)
	mem := pool.classes[len(pool.classes)-1].Pop()
	utest.EqualNow(t, cap(mem), 1024)
	utest.Assert(t, pool.classes[len(pool.classes)-1].head == 0)

	mem = pool.Alloc(1024)
	utest.EqualNow(t, cap(mem), 1024)
}

func Benchmark_AtomPool_AllocAndFree_128(b *testing.B) {
	pool := NewAtomPool(128, 1024, 2, 64*1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Free(pool.Alloc(128))
		}
	})
}

func Benchmark_AtomPool_AllocAndFree_256(b *testing.B) {
	pool := NewAtomPool(128, 1024, 2, 64*1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Free(pool.Alloc(256))
		}
	})
}

func Benchmark_AtomPool_AllocAndFree_512(b *testing.B) {
	pool := NewAtomPool(128, 1024, 2, 64*1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Free(pool.Alloc(512))
		}
	})
}
