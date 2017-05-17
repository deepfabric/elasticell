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

func (s *Store) execKVSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	err := s.getKVEngine().Set(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (s *Store) execKVGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getKVEngine().Get(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		BulkResult: value,
	}
}

func (s *Store) execKVStrLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().StrLen(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execKVIncrBy(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	incrment, err := util.StrInt64(args[1])
	if err != nil {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().IncrBy(args[0], incrment)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execKVIncr(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().IncrBy(args[0], 1)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execKVDecrby(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	incrment, err := util.StrInt64(args[1])
	if err != nil {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().DecrBy(args[0], incrment)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execKVDecr(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().DecrBy(args[0], 1)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execKVGetSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getKVEngine().GetSet(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		BulkResult: value,
	}
}

func (s *Store) execKVAppend(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().Append(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (s *Store) execKVSetNX(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	n, err := s.getKVEngine().SetNX(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}
