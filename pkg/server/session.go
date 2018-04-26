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
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	gedis "github.com/fagongzi/goetty/protocol/redis"
)

type session struct {
	sync.RWMutex

	id int64

	closed bool
	resps  *util.Queue

	conn goetty.IOSession
	addr string

	fromProxy bool
}

func newSession(conn goetty.IOSession) *session {
	return &session{
		id:    conn.ID().(int64),
		resps: &util.Queue{},
		conn:  conn,
		addr:  conn.RemoteAddr(),
	}
}

func (s *session) close() {
	s.Lock()
	resps := s.resps.Dispose()
	for _, resp := range resps {
		pool.ReleaseResponse(resp.(*raftcmdpb.Response))
	}
	log.Debugf("redis-[%s]: closed", s.addr)
	s.Unlock()
}

func (s *session) setFromProxy() {
	s.fromProxy = true
}

func (s *session) onResp(resp *raftcmdpb.Response) {
	if s != nil {
		s.resps.Put(resp)
	} else {
		pool.ReleaseResponse(resp)
	}
}

func (s *session) writeLoop() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("painc: %+v", err)
		}
	}()

	items := make([]interface{}, globalCfg.BatchCliResps, globalCfg.BatchCliResps)

	for {
		// If in the read goroutine, the connection is closed, so we need a lock
		s.RLock()
		n, err := s.resps.Get(globalCfg.BatchCliResps, items)
		if nil != err {
			s.RUnlock()
			return
		}

		buf := s.conn.OutBuf()
		for i := int64(0); i < n; i++ {
			rsp := items[i].(*raftcmdpb.Response)
			s.doResp(rsp, buf)
			pool.ReleaseResponse(rsp)

			if i > 0 && i%globalCfg.BatchCliResps == 0 {
				s.conn.Flush()
			}
		}

		if buf.Readable() > 0 {
			s.conn.Flush()
		}
		s.RUnlock()
	}
}

func (s *session) doResp(resp *raftcmdpb.Response, buf *goetty.ByteBuf) {
	if s.fromProxy {
		size := resp.Size()
		buf.WriteByte(redis.ProxyBegin)
		buf.WriteInt(size)

		index := buf.GetWriteIndex()
		buf.Expansion(size)
		util.MustMarshalTo(resp, buf.RawBuf()[index:index+size])
		buf.SetWriterIndex(index + size)
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

	if resp.DocArrayResult != nil || resp.HasEmptyDocArrayResult != nil {
		redis.WriteDocArray(resp.DocArrayResult, buf)
	}
}
