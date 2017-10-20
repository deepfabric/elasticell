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

func (s *Store) execKVSet(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	ctx.rb.set(args[0], args[1])

	size := uint64(len(args[0]) + len(args[1]))
	ctx.metrics.writtenKeys++
	ctx.metrics.writtenBytes += size
	ctx.metrics.sizeDiffHint += size

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp

	return rsp
}

func (s *Store) execKVGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getKVEngine().Get(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.BulkResult = value
	return rsp
}

func (s *Store) execKVStrLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().StrLen(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execKVIncrBy(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	incrment, err := util.StrInt64(args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().IncrBy(args[0], incrment)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())

		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execKVIncr(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().IncrBy(args[0], 1)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execKVDecrby(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	incrment, err := util.StrInt64(args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().DecrBy(args[0], incrment)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execKVDecr(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().DecrBy(args[0], 1)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execKVGetSet(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getKVEngine().GetSet(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.BulkResult = value
	return rsp
}

func (s *Store) execKVAppend(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().Append(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	size := uint64(len(args[1]))
	ctx.metrics.writtenBytes += size
	ctx.metrics.sizeDiffHint += size

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}

func (s *Store) execKVSetNX(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	n, err := s.getKVEngine().SetNX(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	if n > 0 {
		size := uint64(len(args[0]) + len(args[1]))
		ctx.metrics.writtenKeys++
		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &n
	return rsp
}
