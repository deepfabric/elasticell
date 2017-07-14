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
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
)

func (s *Store) execHSet(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	n, err := s.getHashEngine().HSet(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if n > 0 {
		size := int64(len(args[1]) + len(args[2]))
		ctx.metrics.writtenKeys++
		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execHDel(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	n, err := s.getHashEngine().HDel(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if n > 0 {
		var size int64

		for _, arg := range args[1:] {
			size += int64(len(arg))
		}

		ctx.metrics.sizeDiffHint -= size
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execHMSet(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()
	var size int64

	l := len(args)
	if l < 3 || l%2 == 0 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	key := args[0]
	kvs := args[1:]
	l = len(kvs) / 2
	fields := make([][]byte, l)
	values := make([][]byte, l)

	for i := 0; i < len(kvs); i++ {
		fields[i] = kvs[2*i]
		values[i] = kvs[2*i+1]

		size += int64(len(fields[i]))
		size += int64(len(values[i]))
	}

	err := s.getHashEngine().HMSet(key, fields, values)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	ctx.metrics.writtenKeys++
	ctx.metrics.writtenBytes += size
	ctx.metrics.sizeDiffHint += size

	return &raftcmdpb.Response{
		StatusResult: redis.OKStatusResp,
	}
}

func (s *Store) execHSetNX(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HSetNX(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		size := int64(len(args[1]) + len(args[2]))
		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execHIncrBy(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	incrment, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getHashEngine().HIncrBy(args[0], args[1], incrment)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	v, err := util.StrInt64(value)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}
	return &raftcmdpb.Response{
		IntegerResult: &v,
	}
}

func (s *Store) execHGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HGet(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (s *Store) execHExists(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	exists, err := s.getHashEngine().HExists(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var value int64
	if exists {
		value = 1
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execHKeys(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HKeys(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var has = true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (s *Store) execHVals(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HVals(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var has = true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (s *Store) execHGetAll(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HGetAll(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var has = true
	return &raftcmdpb.Response{
		FvPairArrayResult:         value,
		HasEmptyFVPairArrayResult: &has,
	}
}

func (s *Store) execHLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HLen(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execHMGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, errs := s.getHashEngine().HMGet(args[0], args[1:]...)
	if errs != nil {
		errors := make([][]byte, len(errs))
		for idx, err := range errs {
			// TODO: bug invalid memory address or nil pointer dereference
			errors[idx] = util.StringToSlice(err.Error())
		}

		return &raftcmdpb.Response{
			ErrorResults: errors,
		}
	}

	has := true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (s *Store) execHStrLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return &raftcmdpb.Response{
			ErrorResult: redis.ErrInvalidCommandResp,
		}
	}

	value, err := s.getHashEngine().HStrLen(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}
