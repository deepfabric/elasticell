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
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/errorpb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
)

type cmd struct {
	req  *raftcmdpb.RaftCMDRequest
	cb   func(*raftcmdpb.RaftCMDResponse)
	term uint64
}

func (c *cmd) reset() {
	c.req = nil
	c.cb = nil
	c.term = 0
}

func newCMD(req *raftcmdpb.RaftCMDRequest, cb func(*raftcmdpb.RaftCMDResponse)) *cmd {
	c := acquireCmd()
	c.req = req
	c.cb = cb
	return c
}

func (s *Store) respStoreNotMatch(err error, req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) {
	rsp := errorPbResp(&errorpb.Error{
		Message:       err.Error(),
		StoreNotMatch: storeNotMatch,
	}, uuid.NewV4().Bytes(), 0)

	resp := pool.AcquireResponse()
	resp.UUID = req.UUID
	resp.SessionID = req.SessionID
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func (c *cmd) resp(resp *raftcmdpb.RaftCMDResponse) {
	if c.cb != nil {
		log.Debugf("raftstore[cell-%d]: response to client, resp=<%+v>",
			c.req.Header.CellId,
			resp)

		if len(c.req.Requests) > 0 {
			if len(c.req.Requests) != len(resp.Responses) {
				if resp.Header == nil {
					log.Fatalf("bug: requests and response count not match.")
				} else if len(resp.Responses) != 0 {
					log.Fatalf("bug: responses len must be 0.")
				}

				for _, req := range c.req.Requests {
					rsp := pool.AcquireResponse()
					rsp.UUID = req.UUID
					rsp.SessionID = req.SessionID

					resp.Responses = append(resp.Responses, rsp)
				}
			} else {
				for idx, req := range c.req.Requests {
					resp.Responses[idx].UUID = req.UUID
					resp.Responses[idx].SessionID = req.SessionID
				}
			}

			if resp.Header != nil {
				for _, rsp := range resp.Responses {
					rsp.Error = resp.Header.Error
				}
			}
		}

		log.Debugf("raftstore[cell-%d]: after response to client, resp=<%+v>",
			c.req.Header.CellId,
			resp)
		c.cb(resp)

		if globalCfg.EnableMetricsRequest {
			observeRequestResponse(c)
		}
	} else {
		pool.ReleaseRaftResponseAll(resp)
	}

	c.release()
}

func (c *cmd) release() {
	pool.ReleaseRaftRequestAll(c.req)
	releaseCmd(c)
}

func (c *cmd) respCellNotFound(cellID uint64) {
	err := new(errorpb.CellNotFound)
	err.CellID = cellID

	rsp := errorPbResp(&errorpb.Error{
		Message:      errCellNotFound.Error(),
		CellNotFound: err,
	}, c.req.Header.UUID, c.term)

	c.resp(rsp)
}

func (c *cmd) respLargeRaftEntrySize(cellID uint64, size uint64) {
	err := &errorpb.RaftEntryTooLarge{
		CellID:    cellID,
		EntrySize: size,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:           errLargeRaftEntrySize.Error(),
		RaftEntryTooLarge: err,
	}, c.getUUID(), c.term)

	c.resp(rsp)
}

func (c *cmd) respOtherError(err error) {
	rsp := errorOtherCMDResp(err)
	c.resp(rsp)
}

func (c *cmd) respNotLeader(cellID uint64, leader metapb.Peer) {
	err := &errorpb.NotLeader{
		CellID: cellID,
		Leader: leader,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:   errNotLeader.Error(),
		NotLeader: err,
	}, c.getUUID(), c.term)

	c.resp(rsp)
}

func (c *cmd) getUUID() []byte {
	return c.req.Header.UUID
}

func (pr *PeerReplicate) execReadLocal(c *cmd) {
	pr.doExecReadCmd(c)
	pr.metrics.propose.readLocal++
}

func (pr *PeerReplicate) execReadIndex(c *cmd) {
	if !pr.isLeader() {
		c.respNotLeader(pr.cellID, pr.store.getPeer(pr.getLeaderPeerID()))
		return
	}

	lastPendingReadCount := pr.pendingReadCount()
	lastReadyReadCount := pr.readyReadCount()

	log.Debugf("raftstore[cell-%d]: to read index, cmd=<%+v>",
		pr.cellID,
		c)
	pr.rn.ReadIndex(c.getUUID())

	pendingReadCount := pr.pendingReadCount()
	readyReadCount := pr.readyReadCount()

	if pendingReadCount == lastPendingReadCount &&
		readyReadCount == lastReadyReadCount {
		c.respNotLeader(pr.cellID, pr.store.getPeer(pr.getLeaderPeerID()))
		return
	}

	pr.pendingReads.push(c)
	pr.metrics.propose.readIndex++
}
