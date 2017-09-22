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

package server

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
)

func (s *RedisServer) onHSet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHGet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHDel(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHExists(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHKeys(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHVals(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHGetAll(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHLen(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHMGet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHMSet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	l := len(args)
	if l < 3 || l%2 == 0 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHSetNX(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHStrLen(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onHIncrBy(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}
