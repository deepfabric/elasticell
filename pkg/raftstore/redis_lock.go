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
	"bytes"
	"fmt"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/util/hack"
)

func (s *Store) execLock(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	resource := args[0]
	target := args[1]
	keys := args[2:]

	hash := s.getHashEngine(ctx.req.Header.CellId)
	values, errs := hash.HMGet(resource, keys...)
	if len(errs) > 0 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(errs[0].Error())
		return rsp
	}

	for idx, value := range values {
		if len(value) > 0 && bytes.Compare(value, target) != 0 {
			rsp := pool.AcquireResponse()
			rsp.StatusResult = hack.StringToSlice(fmt.Sprintf("%s already locked by %s", keys[idx], value))
			return rsp
		}
	}

	targets := make([][]byte, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		targets = append(targets, target)
	}
	err := hash.HMSet(resource, keys, targets)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.StatusResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

func (s *Store) execLockable(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	resource := args[0]
	target := args[1]
	keys := args[2:]

	hash := s.getHashEngine(id)
	values, errs := hash.HMGet(resource, keys...)
	if len(errs) > 0 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(errs[0].Error())

		return rsp
	}

	for idx, value := range values {
		if len(value) > 0 && bytes.Compare(value, target) != 0 {
			rsp := pool.AcquireResponse()
			rsp.StatusResult = hack.StringToSlice(fmt.Sprintf("%s already locked by %s", keys[idx], value))
			return rsp
		}
	}

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}

func (s *Store) execUnlock(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	resource := args[0]
	keys := args[1:]

	hash := s.getHashEngine(ctx.req.Header.CellId)
	_, err := hash.HDel(resource, keys...)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.StatusResult = redis.OKStatusResp
	return rsp
}
