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

package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

// command like: bmcreate bm1 [1, 3, 3]
func (s *Store) execBMCreate(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	key := args[0]
	ctx.bitmapBatch.add(key)

	if len(args) > 1 {
		for _, value := range args[1:] {
			v, err := format.ParseStrUInt64(hack.SliceToString(value))
			if err != nil {
				rsp := pool.AcquireResponse()
				rsp.ErrorResult = hack.StringToSlice(err.Error())
				return rsp
			}
			ctx.bitmapBatch.add(key, v)
		}
	}

	ctx.metrics.writtenKeys++
	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

// command like: bmadd bm1 1 [2 3 4]
func (s *Store) execBMAdd(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	key := args[0]
	for _, value := range args[1:] {
		v, err := format.ParseStrUInt64(hack.SliceToString(value))
		if err != nil {
			rsp := pool.AcquireResponse()
			rsp.ErrorResult = hack.StringToSlice(err.Error())
			return rsp
		}
		ctx.bitmapBatch.add(key, v)
	}

	ctx.metrics.writtenKeys++

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

// command like: bmremove bm1 1 [2 3 4]
func (s *Store) execBMRemove(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	key := args[0]
	for _, value := range args[1:] {
		v, err := format.ParseStrUInt64(hack.SliceToString(value))
		if err != nil {
			rsp := pool.AcquireResponse()
			rsp.ErrorResult = hack.StringToSlice(err.Error())
			return rsp
		}
		ctx.bitmapBatch.remove(key, v)
	}

	ctx.metrics.writtenKeys++

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

// command like: bmclear bm1
func (s *Store) execBMClear(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	ctx.bitmapBatch.clear(args[0])
	ctx.metrics.writtenKeys++

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

// command like: bmdel bm1
func (s *Store) execBMDel(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	ctx.bitmapBatch.del(args[0])
	ctx.metrics.writtenKeys++

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

// command like: bmrange bm1 start count
func (s *Store) execBMRange(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	start, err := format.ParseStrUInt64(hack.SliceToString(args[1]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	limit, err := format.ParseStrUInt64(hack.SliceToString(args[2]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getKVEngine(id).Get(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	if len(value) == 0 {
		rsp.HasEmptySliceArrayResult = true
	} else {
		bm := acquireBitmap()
		_, _, err := bm.ImportRoaringBits(value, false, false, 0)
		if err != nil {
			rsp.ErrorResult = hack.StringToSlice(err.Error())
			releaseBitmap(bm)
			return rsp
		}

		var values [][]byte
		count := uint64(0)
		itr := bm.Iterator()
		itr.Seek(start)
		for {
			value, eof := itr.Next()
			if eof {
				break
			}

			values = append(values, format.UInt64ToString(value))
			count++

			if count >= limit {
				break
			}
		}
		rsp.SliceArrayResult = values
		rsp.HasEmptySliceArrayResult = len(values) == 0
		releaseBitmap(bm)
	}

	return rsp
}

// command like: bmcount bm1
func (s *Store) execBMCount(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getKVEngine(id).Get(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	if len(value) == 0 {
		n := int64(0)
		rsp.IntegerResult = &n
	} else {
		bm := acquireBitmap()
		_, _, err := bm.ImportRoaringBits(value, false, false, 0)
		if err != nil {
			rsp.ErrorResult = hack.StringToSlice(err.Error())
			releaseBitmap(bm)
			return rsp
		}

		n := int64(bm.Count())
		rsp.IntegerResult = &n
		releaseBitmap(bm)
	}

	return rsp
}

// command like: bmcontains bm1 1
func (s *Store) execBMContains(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	id, err := format.ParseStrUInt64(hack.SliceToString(args[1]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getKVEngine(id).Get(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	var n int64
	rsp := pool.AcquireResponse()

	if len(value) > 0 {
		bm := acquireBitmap()
		_, _, err := bm.ImportRoaringBits(value, false, false, 0)
		if err != nil {
			rsp.ErrorResult = hack.StringToSlice(err.Error())
			releaseBitmap(bm)
			return rsp
		}

		n = 0
		if bm.Contains(id) {
			n = 1
		}

		releaseBitmap(bm)
	}

	v := n
	rsp.IntegerResult = &v
	return rsp
}
