Introduction
============

[![Go Report Card](https://goreportcard.com/badge/github.com/funny/slab)](https://goreportcard.com/report/github.com/funny/slab)
[![Build Status](https://travis-ci.org/funny/slab.svg?branch=master)](https://travis-ci.org/funny/slab)
[![codecov](https://codecov.io/gh/funny/slab/branch/master/graph/badge.svg)](https://codecov.io/gh/funny/slab)
[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg)](https://godoc.org/github.com/funny/slab)

Slab allocation memory pools for Go.

Usage
=====

Use lock-free memory pool:

```go
pool := slab.NewAtomPool(
	64,          // The smallest chunk size is 64B.
	64 * 1024,   // The largest chunk size is 64KB.
	2,           // Power of 2 growth in chunk size.
	1024 * 1024, // Each slab will be 1MB in size.
)

buf1 := pool.Alloc(64)

    ... use the buf ...
	
pool.Free(buf)
```

Use `chan` based memory pool:

```go
pool := slab.NewChanPool(
	64,          // The smallest chunk size is 64B.
	64 * 1024,   // The largest chunk size is 64KB.
	2,           // Power of 2 growth in chunk size.
	1024 * 1024, // Each slab will be 1MB in size.
)

buf1 := pool.Alloc(64)

    ... use the buf ...
	
pool.Free(buf)
```

Use `sync.Pool` based memory pool:

```go
pool := slab.NewSyncPool(
	64,          // The smallest chunk size is 64B.
	64 * 1024,   // The largest chunk size is 64KB.
	2,           // Power of 2 growth in chunk size.
)

buf := pool.Alloc(64)

    ... use the buf ...
	
pool.Free(buf)
```

Performance
===========

Result of `GOMAXPROCS=16 go test -v -bench=. -benchmem`:

```
Benchmark_AtomPool_AllocAndFree_128-8   	20000000	       104 ns/op	       0 B/op	       0 allocs/op
Benchmark_AtomPool_AllocAndFree_256-8   	20000000	       106 ns/op	       0 B/op	       0 allocs/op
Benchmark_AtomPool_AllocAndFree_512-8   	20000000	       109 ns/op	       0 B/op	       0 allocs/op

Benchmark_ChanPool_AllocAndFree_128-8   	 5000000	       246 ns/op	       0 B/op	       0 allocs/op
Benchmark_ChanPool_AllocAndFree_256-8   	 5000000	       221 ns/op	       0 B/op	       0 allocs/op
Benchmark_ChanPool_AllocAndFree_512-8   	 5000000	       304 ns/op	       0 B/op	       0 allocs/op

Benchmark_LockPool_AllocAndFree_128-8   	 5000000	       275 ns/op	       0 B/op	       0 allocs/op
Benchmark_LockPool_AllocAndFree_256-8   	 5000000	       265 ns/op	       0 B/op	       0 allocs/op
Benchmark_LockPool_AllocAndFree_512-8   	 5000000	       280 ns/op	       0 B/op	       0 allocs/op

Benchmark_SyncPool_AllocAndFree_128-8   	100000000	        22.9 ns/op	      32 B/op	       1 allocs/op
Benchmark_SyncPool_AllocAndFree_256-8   	100000000	        23.4 ns/op	      32 B/op	       1 allocs/op
Benchmark_SyncPool_AllocAndFree_512-8   	50000000	        23.3 ns/op	      32 B/op	       1 allocs/op

Benchmark_SyncPool_CacheMiss_128-8      	10000000	       196 ns/op	     160 B/op	       2 allocs/op
Benchmark_SyncPool_CacheMiss_256-8      	 5000000	       228 ns/op	     288 B/op	       2 allocs/op
Benchmark_SyncPool_CacheMiss_512-8      	10000000	       251 ns/op	     544 B/op	       2 allocs/op

Benchmark_Make_128-8                    	100000000	        17.9 ns/op	     128 B/op	       1 allocs/op
Benchmark_Make_256-8                    	50000000	        31.7 ns/op	     256 B/op	       1 allocs/op
Benchmark_Make_512-8                    	20000000	        63.6 ns/op	     512 B/op	       1 allocs/op
