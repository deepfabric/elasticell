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

func (s *Store) execLIndex(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	index, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getListEngine().LIndex(args[0], index)
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

func (s *Store) execLLEN(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().LLen(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execLRange(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	start, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	end, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getListEngine().LRange(args[0], start, end)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (s *Store) execLInsert(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 4 {
		return redis.ErrInvalidCommandResp
	}

	pos, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getListEngine().LInsert(args[0], int(pos), args[2], args[3])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		size := int64(len(args[3]))

		if value == 0 {
			size += int64(len(args[0]))
			ctx.metrics.writtenKeys++
		}

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execLPop(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().LPop(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	size := int64(len(value))
	ctx.metrics.sizeDiffHint -= size

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (s *Store) execLPush(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().LPush(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		var size int64
		for _, arg := range args[1:] {
			size += int64(len(arg))
		}

		if value == 1 {
			size += int64(len(args[0]))
			ctx.metrics.writtenKeys++
		}

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execLPushX(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().LPushX(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		var size int64

		size += int64(len(args[1]))
		if value == 1 {
			size += int64(len(args[0]))
			ctx.metrics.writtenKeys++
		}

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execLRem(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	count, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getListEngine().LRem(args[0], count, args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		size := int64(len(args[2])) * value
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execLSet(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	index, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	err = s.getListEngine().LSet(args[0], index, args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (s *Store) execLTrim(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	begin, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	end, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	err = s.getListEngine().LTrim(args[0], begin, end)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (s *Store) execRPop(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().RPop(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	size := int64(len(value))
	ctx.metrics.sizeDiffHint -= size

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (s *Store) execRPush(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().RPush(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		var size int64
		for _, arg := range args[1:] {
			size += int64(len(arg))
		}

		if value == 1 {
			size += int64(len(args[0]))
			ctx.metrics.writtenKeys++
		}

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execRPushX(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getListEngine().RPushX(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value > 0 {
		var size int64

		size += int64(len(args[1]))
		if value == 1 {
			size += int64(len(args[0]))
			ctx.metrics.writtenKeys++
		}

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}
