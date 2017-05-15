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
	"github.com/deepfabric/elasticell/pkg/redis"
)

func (s *RedisServer) onSet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 2 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onGet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 1 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onIncrBy(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 2 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onIncr(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 1 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onDecrby(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 2 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onDecr(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 1 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onGetSet(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 2 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onAppend(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 2 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onSetNX(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 2 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}

func (s *RedisServer) onStrLen(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 1 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}
