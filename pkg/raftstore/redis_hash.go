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
	"github.com/deepfabric/elasticell/pkg/util"
)

func (s *Store) execHSet(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getHashEngine().HSet(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	if n > 0 {
		size := int64(len(args[1]) + len(args[2]))
		ctx.metrics.writtenKeys++
		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execHDel(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getHashEngine().HDel(args[0], args[1:]...)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	if n > 0 {
		var size int64

		for _, arg := range args[1:] {
			size += int64(len(arg))
		}

		ctx.metrics.sizeDiffHint -= size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execHMSet(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()
	var size int64

	l := len(args)
	if l < 3 || l%2 == 0 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	key := args[0]
	kvs := args[1:]
	l = len(kvs) / 2
	fields := make([][]byte, l)
	values := make([][]byte, l)

	for i := 0; i < l; i++ {
		fields[i] = kvs[2*i]
		values[i] = kvs[2*i+1]

		size += int64(len(fields[i]))
		size += int64(len(values[i]))
	}

	err := s.getHashEngine().HMSet(key, fields, values)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	ctx.metrics.writtenKeys++
	ctx.metrics.writtenBytes += size
	ctx.metrics.sizeDiffHint += size

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp

	return rsp
}

func (s *Store) execHSetNX(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HSetNX(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	if value > 0 {
		size := int64(len(args[1]) + len(args[2]))
		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execHIncrBy(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	incrment, err := util.StrInt64(args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	value, err := s.getHashEngine().HIncrBy(args[0], args[1], incrment)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	v, err := util.StrInt64(value)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &v
	return rsp
}

func (s *Store) execHGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HGet(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	has := true
	rsp := pool.AcquireResponse()
	rsp.BulkResult = value
	rsp.HasEmptyBulkResult = &has

	return rsp
}

func (s *Store) execHExists(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	exists, err := s.getHashEngine().HExists(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	var value int64
	if exists {
		value = 1
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execHKeys(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HKeys(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	var has = true
	rsp := pool.AcquireResponse()
	rsp.SliceArrayResult = value
	rsp.HasEmptySliceArrayResult = &has

	return rsp
}

func (s *Store) execHVals(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HVals(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	var has = true
	rsp := pool.AcquireResponse()
	rsp.SliceArrayResult = value
	rsp.HasEmptySliceArrayResult = &has

	return rsp
}

func (s *Store) execHGetAll(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HGetAll(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	var has = true
	rsp := pool.AcquireResponse()
	rsp.FvPairArrayResult = value
	rsp.HasEmptyFVPairArrayResult = &has

	return rsp
}

func (s *Store) execHLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HLen(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execHMGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, errs := s.getHashEngine().HMGet(args[0], args[1:]...)
	if len(errs) > 0 {
		errors := make([][]byte, len(errs))
		for idx, err := range errs {
			errors[idx] = util.StringToSlice(err.Error())
		}

		rsp := pool.AcquireResponse()
		rsp.ErrorResults = errors

		return rsp
	}

	has := true
	rsp := pool.AcquireResponse()
	rsp.SliceArrayResult = value
	rsp.HasEmptySliceArrayResult = &has

	return rsp

}

func (s *Store) execHStrLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getHashEngine().HStrLen(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}
