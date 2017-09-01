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
	"github.com/Workiva/go-datastructures/queue"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	gedis "github.com/fagongzi/goetty/protocol/redis"
)

type session struct {
	closed bool
	resps  *queue.Queue

	conn goetty.IOSession
	addr string

	fromProxy bool
}

func newSession(conn goetty.IOSession) *session {
	return &session{
		resps: &queue.Queue{},
		conn:  conn,
		addr:  conn.RemoteAddr(),
	}
}

func (s *session) close() {
	s.resps.Dispose()
	log.Debugf("redis-[%s]: closed", s.addr)
}

func (s *session) setFromProxy() {
	s.fromProxy = true
}

func (s *session) onResp(header *raftcmdpb.RaftResponseHeader, resp *raftcmdpb.Response) {
	if header != nil {
		if header.Error.RaftEntryTooLarge == nil {
			resp.Type = raftcmdpb.RaftError
		} else {
			resp.Type = raftcmdpb.Invalid
		}

		resp.ErrorResult = util.MustMarshal(header)
	}

	if s != nil {
		s.resps.Put(resp)
	}
}

func (s *session) writeLoop() {
	for {
		resps, err := s.resps.Get(globalCfg.Redis.WriteBatchLimit)
		if nil != err {
			return
		}

		buf := s.conn.OutBuf()
		for _, resp := range resps {
			s.doResp(resp.(*raftcmdpb.Response), buf)
		}
		s.conn.WriteOutBuf()
	}
}

func (s *session) doResp(resp *raftcmdpb.Response, buf *goetty.ByteBuf) {
	if s.fromProxy {
		data := util.MustMarshal(resp)
		buf.WriteByte(redis.ProxyBegin)
		buf.WriteInt(len(data))
		buf.Write(data)
		return
	}

	if resp.ErrorResult != nil {
		gedis.WriteError(resp.ErrorResult, buf)
	}

	if resp.ErrorResults != nil {
		for _, err := range resp.ErrorResults {
			gedis.WriteError(err, buf)
		}
	}

	if resp.BulkResult != nil || resp.HasEmptyBulkResult != nil {
		gedis.WriteBulk(resp.BulkResult, buf)
	}

	if resp.FvPairArrayResult != nil || resp.HasEmptyFVPairArrayResult != nil {
		redis.WriteFVPairArray(resp.FvPairArrayResult, buf)
	}

	if resp.IntegerResult != nil {
		gedis.WriteInteger(*resp.IntegerResult, buf)
	}

	if resp.ScorePairArrayResult != nil || resp.HasEmptyScorePairArrayResult != nil {
		redis.WriteScorePairArray(resp.ScorePairArrayResult, *resp.Withscores, buf)
	}

	if resp.SliceArrayResult != nil || resp.HasEmptySliceArrayResult != nil {
		gedis.WriteSliceArray(resp.SliceArrayResult, buf)
	}

	if resp.StatusResult != nil {
		gedis.WriteStatus(resp.StatusResult, buf)
	}
}
