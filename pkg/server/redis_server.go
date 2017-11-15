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
	"io"
	"strings"
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/raftstore"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

// RedisServer is provide a redis like server
type RedisServer struct {
	sync.RWMutex

	store         *raftstore.Store
	s             *goetty.Server
	typeMapping   map[string]raftcmdpb.CMDType
	localHandlers map[raftcmdpb.CMDType]func(*raftcmdpb.Request, *session) ([]byte, error)
	handlers      map[raftcmdpb.CMDType]func(raftcmdpb.CMDType, redis.Command, *session) ([]byte, error)
	routing       *routing
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
	s.routing = newRouting()
	s.localHandlers = make(map[raftcmdpb.CMDType]func(*raftcmdpb.Request, *session) ([]byte, error))
	s.handlers = make(map[raftcmdpb.CMDType]func(raftcmdpb.CMDType, redis.Command, *session) ([]byte, error))
	s.typeMapping = make(map[string]raftcmdpb.CMDType)

	for k, v := range raftcmdpb.CMDType_value {
		s.typeMapping[strings.ToLower(k)] = raftcmdpb.CMDType(v)
	}

	s.localHandlers[raftcmdpb.Ping] = s.onPing

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

	// zset
	s.handlers[raftcmdpb.ZAdd] = s.onZAdd
	s.handlers[raftcmdpb.ZCard] = s.onZCard
	s.handlers[raftcmdpb.ZCount] = s.onZCount
	s.handlers[raftcmdpb.ZIncrBy] = s.onZIncrBy
	s.handlers[raftcmdpb.ZLexCount] = s.onZLexCount
	s.handlers[raftcmdpb.ZRange] = s.onZRange
	s.handlers[raftcmdpb.ZRangeByLex] = s.onZRangeByLex
	s.handlers[raftcmdpb.ZRangeByScore] = s.onZRangeByScore
	s.handlers[raftcmdpb.ZRank] = s.onZRank
	s.handlers[raftcmdpb.ZRem] = s.onZRem
	s.handlers[raftcmdpb.ZRemRangeByLex] = s.onZRemRangeByLex
	s.handlers[raftcmdpb.ZRemRangeByRank] = s.onZRemRangeByRank
	s.handlers[raftcmdpb.ZRemRangeByScore] = s.onZRemRangeByScore
	s.handlers[raftcmdpb.ZScore] = s.onZScore
}

func (s *RedisServer) doConnection(session goetty.IOSession) error {
	addr := session.RemoteAddr()
	log.Debugf("redis-[%s]: connected", addr)

	// every client has 2 goroutines, read and write
	rs := newSession(session)
	s.routing.put(rs.id, rs)

	go rs.writeLoop()
	defer func() {
		s.routing.delete(rs.id)
		rs.close()
	}()

	for {
		value, err := session.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			log.Errorf("redis-[%s]: read from cli failed, errors\n %+v",
				addr,
				err)
			return err
		}

		if req, ok := value.(redis.Command); ok {
			log.Debugf("redis-[%s]: read a redis command, cmd=<%+v>", addr, req)

			err = s.onRedisCommand(req, rs)
			if err != nil {
				log.Debugf("onRedisCommand faied. req=<%+v>, err=<%+v>", req, err)
				rsp := pool.AcquireResponse()
				rsp.ErrorResult = util.StringToSlice(err.Error())
				rs.onResp(rsp)
			}
		} else if req, ok := value.(*raftcmdpb.Request); ok {
			if len(req.UUID) > 0 {
				log.Debugf("req: read a raft req. from=<%s>, req=<%v>",
					addr,
					req)
			}

			rs.setFromProxy()
			err = s.onProxyReq(req, rs)
			if err != nil {
				log.Debugf("onProxyReq faied. req=<%+v>, err=<%+v>", req, err)
				rsp := pool.AcquireResponse()
				rsp.ErrorResult = util.StringToSlice(err.Error())
				rsp.UUID = req.UUID
				rs.onResp(rsp)
				pool.ReleaseRequest(req)
			}
		}
	}
}

func (s *RedisServer) onResp(resp *raftcmdpb.RaftCMDResponse) {
	var errorResult []byte
	hasError := resp.Header != nil

	for _, rsp := range resp.Responses {
		rs := s.routing.get(rsp.SessionID)
		if rs != nil {
			if hasError {
				if resp.Header.Error.RaftEntryTooLarge == nil {
					rsp.Type = raftcmdpb.RaftError
				} else {
					rsp.Type = raftcmdpb.Invalid
				}

				if errorResult == nil {
					errorResult = util.MustMarshal(resp.Header)
				}

				rsp.ErrorResult = errorResult
			}

			rs.onResp(rsp)
		} else {
			pool.ReleaseResponse(rsp)
		}
	}

	pool.ReleaseRaftCMDResponse(resp)
}

func (s *RedisServer) onProxyReq(req *raftcmdpb.Request, session *session) error {
	req.Type = s.typeMapping[strings.ToLower(util.SliceToString(req.Cmd[0]))]
	req.SessionID = session.id

	if h, ok := s.localHandlers[req.Type]; ok {
		h(req, session)
		return nil
	}

	if len(req.Cmd) < 2 {
		rsp := pool.AcquireResponse()
		rsp.UUID = req.UUID
		rsp.ErrorResult = redis.ErrNotSupportCommand
		session.onResp(rsp)
		return nil
	}

	err := s.store.OnProxyReq(req, s.onResp)
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisServer) onRedisCommand(cmd redis.Command, session *session) error {
	t := s.typeMapping[cmd.CmdString()]

	if h, ok := s.localHandlers[t]; ok {
		req := pool.AcquireRequest()
		req.Cmd = cmd
		h(req, session)
		return nil
	}

	h, ok := s.handlers[t]
	if !ok {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrNotSupportCommand
		session.onResp(rsp)
		return nil
	}

	_, err := h(t, cmd, session)
	if err != nil {
		return err
	}

	return nil
}

func (s *RedisServer) onDel(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

func (s *RedisServer) onPing(req *raftcmdpb.Request, session *session) ([]byte, error) {
	rsp := pool.AcquireResponse()
	rsp.UUID = req.UUID
	rsp.StatusResult = redis.PongResp
	session.onResp(rsp)
	return nil, nil
}
