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
	"strings"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/raftstore"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

// RedisServer is provide a redis like server
type RedisServer struct {
	store       *raftstore.Store
	s           *goetty.Server
	typeMapping map[string]raftcmdpb.CMDType
	handlers    map[raftcmdpb.CMDType]func(raftcmdpb.CMDType, redis.Command, *session) error
}

// Start used for start the redis server
func (s *RedisServer) Start() error {
	return s.s.Start(s.doConnection)
}

// Stop is used for stop redis server
func (s *RedisServer) Stop() error {
	s.s.Stop()
	return nil
}

func (s *RedisServer) init() {
	s.handlers = make(map[raftcmdpb.CMDType]func(raftcmdpb.CMDType, redis.Command, *session) error)
	s.typeMapping = make(map[string]raftcmdpb.CMDType)

	for k, v := range raftcmdpb.AdminCmdType_value {
		s.typeMapping[strings.ToLower(k)] = raftcmdpb.CMDType(v)
	}

	// server
	s.handlers[raftcmdpb.Del] = s.onDel

	// kv
	s.handlers[raftcmdpb.Set] = s.onSet
	s.handlers[raftcmdpb.Get] = s.onGet
	s.handlers[raftcmdpb.Incrby] = s.onIncrBy
	s.handlers[raftcmdpb.Incr] = s.onIncr
	s.handlers[raftcmdpb.Decrby] = s.onDecrby
	s.handlers[raftcmdpb.Decr] = s.onDecr
	s.handlers[raftcmdpb.GetSet] = s.onGetSet
	s.handlers[raftcmdpb.Append] = s.onAppend
	s.handlers[raftcmdpb.Setnx] = s.onSetNX
	s.handlers[raftcmdpb.StrLen] = s.onStrLen

	// hash
	s.handlers[raftcmdpb.HSet] = s.onHSet
	s.handlers[raftcmdpb.HDel] = s.onHDel
	s.handlers[raftcmdpb.HExists] = s.onHExists
	s.handlers[raftcmdpb.HGet] = s.onHGet
	s.handlers[raftcmdpb.HGetAll] = s.onHGetAll
	s.handlers[raftcmdpb.HIncrBy] = s.onHIncrBy
	s.handlers[raftcmdpb.HKeys] = s.onHKeys
	s.handlers[raftcmdpb.HLen] = s.onHLen
	s.handlers[raftcmdpb.HMGet] = s.onHMGet
	s.handlers[raftcmdpb.HMSet] = s.onHMSet
	s.handlers[raftcmdpb.HSetNX] = s.onHSetNX
	s.handlers[raftcmdpb.HStrLen] = s.onHStrLen
	s.handlers[raftcmdpb.HVals] = s.onHVals

	// list
	s.handlers[raftcmdpb.LIndex] = s.onLIndex
	s.handlers[raftcmdpb.LInsert] = s.onLInsert
	s.handlers[raftcmdpb.LLEN] = s.onLLen
	s.handlers[raftcmdpb.LPop] = s.onLPop
	s.handlers[raftcmdpb.LPush] = s.onLPush
	s.handlers[raftcmdpb.LPushX] = s.onLPushX
	s.handlers[raftcmdpb.LRange] = s.onLRange
	s.handlers[raftcmdpb.LRem] = s.onLRem
	s.handlers[raftcmdpb.LSet] = s.onLSet
	s.handlers[raftcmdpb.LTrim] = s.onLTrim
	s.handlers[raftcmdpb.RPop] = s.onRPop
	s.handlers[raftcmdpb.RPush] = s.onRPush
	s.handlers[raftcmdpb.RPushX] = s.onRPushX

	// sets
	s.handlers[raftcmdpb.SAdd] = s.onSAdd
	s.handlers[raftcmdpb.SCard] = s.onSCard
	s.handlers[raftcmdpb.SRem] = s.onSRem
	s.handlers[raftcmdpb.SMembers] = s.onSMembers
	s.handlers[raftcmdpb.SIsMember] = s.onSIsMember
	s.handlers[raftcmdpb.SPop] = s.onSPop
}

func (s *RedisServer) doConnection(session goetty.IOSession) error {
	// every client has 2 goroutines, read and write
	rs := newSession(session)
	go rs.writeLoop()
	defer rs.close()

	for {
		req, err := session.Read()
		if err != nil {
			return err
		}

		err = s.onRedisCommand(req.(redis.Command), rs)
		if err != nil {
			rs.onResp(&raftcmdpb.Response{
				ErrorResult: util.StringToSlice(err.Error()),
			})
		}
	}
}

func (s *RedisServer) onRedisCommand(cmd redis.Command, session *session) error {
	t := s.typeMapping[cmd.CmdString()]
	h, ok := s.handlers[t]
	if !ok {
		session.onResp(redis.ErrNotSupportCommand)
		return nil
	}

	return h(t, cmd, session)
}

func (s *RedisServer) onDel(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) error {
	args := cmd.Args()
	if len(args) != 1 {
		session.onResp(redis.ErrInvalidCommandResp)
		return nil
	}

	return s.store.OnRedisCommand(cmdType, cmd, session.respCB)
}
