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
	"errors"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/errorpb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
)

var (
	errStaleCMD           = errors.New("stale command")
	errStaleEpoch         = errors.New("stale epoch")
	errNotLeader          = errors.New("NotLeader")
	errCellNotFound       = errors.New("cell not found")
	errMissingUUIDCMD     = errors.New("missing request uuid")
	errLargeRaftEntrySize = errors.New("entry is too large")
	errKeyNotInCell       = errors.New("key not in cell")
	errKeyNotInStore      = errors.New("key not in store")

	infoStaleCMD = new(errorpb.StaleCommand)
)

type splitCheckResult struct {
	cellID   uint64
	epoch    metapb.CellEpoch
	splitKey []byte
}

func buildTerm(term uint64, resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header == nil {
		return
	}

	resp.Header.CurrentTerm = term
}

func buildUUID(uuid []byte, resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header == nil {
		return
	}

	if resp.Header.UUID != nil {
		resp.Header.UUID = uuid
	}
}

func errorOtherCMDResp(err error) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(nil, 0)
	resp.Header.Error.Message = err.Error()
	return resp
}

func errorPbResp(err *errorpb.Error, uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)
	resp.Header.Error = *err

	return resp
}

func errorStaleCMDResp(uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)
	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleCommand = infoStaleCMD

	return resp
}

func errorStaleEpochResp(uuid []byte, currentTerm uint64, newCells ...metapb.Cell) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)

	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleEpoch = &errorpb.StaleEpoch{
		NewCells: newCells,
	}

	return resp
}

func errorBaseResp(uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := new(raftcmdpb.RaftCMDResponse)
	resp.Header = new(raftcmdpb.RaftResponseHeader)
	buildTerm(currentTerm, resp)
	buildUUID(uuid, resp)

	return resp
}

type cmd struct {
	req *raftcmdpb.RaftCMDRequest
	cb  func(*raftcmdpb.RaftCMDResponse)
}

func newCMD(req *raftcmdpb.RaftCMDRequest, cb func(*raftcmdpb.RaftCMDResponse)) *cmd {
	return &cmd{
		req: req,
		cb:  cb,
	}
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
					resp.Responses = append(resp.Responses, &raftcmdpb.Response{
						UUID: req.UUID,
					})
				}
			} else {
				for idx, req := range c.req.Requests {
					resp.Responses[idx].UUID = req.UUID
				}
			}
		}

		if resp.Header != nil {
			for idx, req := range c.req.Requests {
				req.Cmd[1] = getOriginKey(req.Cmd[1])
				resp.Responses[idx].OriginRequest = req
			}
		}

		log.Debugf("raftstore[cell-%d]: after response to client, resp=<%+v>",
			c.req.Header.CellId,
			resp)
		c.cb(resp)
	}
}

func (c *cmd) respCellNotFound(cellID uint64, term uint64) {
	err := new(errorpb.CellNotFound)
	err.CellID = cellID

	rsp := errorPbResp(&errorpb.Error{
		Message:      errCellNotFound.Error(),
		CellNotFound: err,
	}, c.req.Header.UUID, term)

	c.resp(rsp)
}

func (c *cmd) respLargeRaftEntrySize(cellID uint64, size uint64, term uint64) {
	err := &errorpb.RaftEntryTooLarge{
		CellID:    cellID,
		EntrySize: size,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:           errLargeRaftEntrySize.Error(),
		RaftEntryTooLarge: err,
	}, c.getUUID(), term)

	c.resp(rsp)
}

func (c *cmd) respOtherError(err error) {
	rsp := errorOtherCMDResp(err)
	c.resp(rsp)
}

func (c *cmd) respNotLeader(cellID uint64, term uint64, leader *metapb.Peer) {
	err := &errorpb.NotLeader{
		CellID: cellID,
	}

	if leader != nil {
		err.Leader = *leader
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:   errNotLeader.Error(),
		NotLeader: err,
	}, c.getUUID(), term)

	c.resp(rsp)
}

func (c *cmd) getUUID() []byte {
	return c.req.Header.UUID
}

func (pr *PeerReplicate) execReadLocal(cmd *cmd) {
	pr.doExecReadCmd(cmd)
}

func (pr *PeerReplicate) execReadIndex(meta *proposalMeta) {
	if !pr.isLeader() {
		meta.cmd.respNotLeader(pr.cellID, meta.term, nil)
		return
	}

	lastPendingReadCount := pr.pendingReadCount()
	lastReadyReadCount := pr.readyReadCount()

	pr.rn.ReadIndex(meta.cmd.getUUID())

	pendingReadCount := pr.pendingReadCount()
	readyReadCount := pr.readyReadCount()

	if pendingReadCount == lastPendingReadCount &&
		readyReadCount == lastReadyReadCount {
		// The message gets dropped silently, can't be handled anymore.
		meta.cmd.respNotLeader(pr.cellID, meta.term, nil)
		return
	}

	pr.pendingReads.push(meta.cmd)
	pr.raftReady()
}
