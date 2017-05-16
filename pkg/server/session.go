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
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"golang.org/x/net/context"
)

type session struct {
	ctx    context.Context
	cancel context.CancelFunc
	resps  chan *raftcmdpb.Response
	conn   goetty.IOSession
}

func newSession(conn goetty.IOSession) *session {
	ctx, cancel := context.WithCancel(context.TODO())

	return &session{
		ctx:    ctx,
		cancel: cancel,
		resps:  make(chan *raftcmdpb.Response, 32),
		conn:   conn,
	}
}

func (s *session) close() {
	s.cancel()
	close(s.resps)
}

func (s *session) respCB(resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header != nil {
		s.doResp(&raftcmdpb.Response{
			Type:        raftcmdpb.Invalid,
			ErrorResult: util.MustMarshal(resp.Header),
		})
	} else {
		for _, rsp := range resp.Responses {
			s.doResp(rsp)
		}
	}
}

func (s *session) onResp(resp *raftcmdpb.Response) {
	s.resps <- resp
}

func (s *session) writeLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case resp := <-s.resps:
			s.doResp(resp)
		}
	}
}

func (s *session) doResp(resp *raftcmdpb.Response) {
	buf := s.conn.OutBuf()

	if resp.ErrorResult != nil {
		redis.WriteError(resp.ErrorResult, buf)
	}

	if resp.ErrorResults != nil {
		for _, err := range resp.ErrorResults {
			redis.WriteError(err, buf)
		}
	}

	if resp.BulkResult != nil || resp.HasEmptyBulkResult != nil {
		redis.WriteBulk(resp.ErrorResult, buf)
	}

	if resp.FvPairArrayResult != nil || resp.HasEmptyFVPairArrayResult != nil {
		redis.WriteFVPairArray(resp.FvPairArrayResult, buf)
	}

	if resp.IntegerResult != nil {
		redis.WriteInteger(*resp.IntegerResult, buf)
	}

	if resp.ScorePairArrayResult != nil || resp.HasEmptyScorePairArrayResult != nil {
		redis.WriteScorePairArray(resp.ScorePairArrayResult, *resp.Withscores, buf)
	}

	if resp.SliceArrayResult != nil || resp.HasEmptySliceArrayResult != nil {
		redis.WriteSliceArray(resp.SliceArrayResult, buf)
	}

	if resp.StatusResult != nil {
		redis.WriteStatus(resp.StatusResult, buf)
	}

	s.conn.WriteOutBuf()
}
