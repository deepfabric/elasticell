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
	"errors"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty/protocol/redis"
)

type redisBatch struct {
	kvKeys   [][]byte
	kvValues [][]byte
}

func (rb *redisBatch) set(key, value []byte) {
	rb.kvKeys = append(rb.kvKeys, key)
	rb.kvValues = append(rb.kvValues, value)
}

func (rb *redisBatch) hasSetBatch() bool {
	return len(rb.kvKeys) > 0
}

func (rb *redisBatch) reset() {
	rb.kvKeys = make([][]byte, 0)
	rb.kvValues = make([][]byte, 0)
}

type applyContext struct {
	wb storage.WriteBatch
	rb *redisBatch

	applyState mraft.RaftApplyState
	req        *raftcmdpb.RaftCMDRequest
	index      uint64
	term       uint64
	metrics    applyMetrics
}

func (ctx *applyContext) reset() {
	ctx.wb = nil
	ctx.applyState = emptyApplyState
	ctx.req = nil
	ctx.index = 0
	ctx.term = 0
	ctx.metrics = emptyApplyMetrics
}

func (d *applyDelegate) checkEpoch(req *raftcmdpb.RaftCMDRequest) bool {
	return checkEpoch(d.cell, req)
}

func (d *applyDelegate) doApplyRaftCMD(ctx *applyContext) *execResult {
	if ctx.index == 0 {
		log.Fatalf("raftstore-apply[cell-%d]: apply raft command needs a none zero index",
			d.cell.ID)
	}

	c := d.findCB(ctx)

	if c != nil && globalCfg.EnableMetricsRequest {
		observeRequestRaft(c)
	}

	if d.isPendingRemove() {
		log.Fatalf("raftstore-apply[cell-%d]: apply raft comand can not pending remove",
			d.cell.ID)
	}

	var err error
	var resp *raftcmdpb.RaftCMDResponse
	var result *execResult

	ctx.wb = d.store.engine.NewWriteBatch()
	ctx.rb = acquireRedisBatch()

	if !d.checkEpoch(ctx.req) {
		resp = errorStaleEpochResp(ctx.req.Header.UUID, d.term, d.cell)
	} else {
		if ctx.req.AdminRequest != nil {
			resp, result, err = d.execAdminRequest(ctx)
			if err != nil {
				resp = errorStaleEpochResp(ctx.req.Header.UUID, d.term, d.cell)
			}
		} else {
			resp = d.execWriteRequest(ctx)
		}
	}

	if ctx.rb.hasSetBatch() {
		err = d.store.getKVEngine().MSet(ctx.rb.kvKeys, ctx.rb.kvValues)
		if err != nil {
			log.Fatalf("raftstore-apply[cell-%d]: save apply context failed, errors:\n %+v",
				d.cell.ID,
				err)
		}

		releaseRedisBatch(ctx.rb)
		ctx.rb = nil
	}

	ctx.applyState.AppliedIndex = ctx.index
	if !d.isPendingRemove() {
		err := ctx.wb.Set(getApplyStateKey(d.cell.ID), util.MustMarshal(&ctx.applyState))
		if err != nil {
			log.Fatalf("raftstore-apply[cell-%d]: save apply context failed, errors:\n %+v",
				d.cell.ID,
				err)
		}
	}

	err = d.store.engine.Write(ctx.wb)
	if err != nil {
		log.Fatalf("raftstore-apply[cell-%d]: commit apply result failed, errors:\n %+v",
			d.cell.ID,
			err)
	}

	d.applyState = ctx.applyState
	d.term = ctx.term
	log.Debugf("raftstore-apply[cell-%d]: applied command, uuid=<%v> index=<%d> resp=<%+v> state=<%+v>",
		d.cell.ID,
		ctx.req.Header.UUID,
		ctx.index,
		resp,
		d.applyState)

	if c != nil {
		if globalCfg.EnableMetricsRequest {
			observeRequestStored(c)
		}

		if resp != nil {
			buildTerm(d.term, resp)
			buildUUID(ctx.req.Header.UUID, resp)

			// resp client
			c.resp(resp)
		}
	}

	return result
}

func (d *applyDelegate) execAdminRequest(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	cmdType := ctx.req.AdminRequest.Type
	switch cmdType {
	case raftcmdpb.ChangePeer:
		return d.doExecChangePeer(ctx)
	case raftcmdpb.Split:
		return d.doExecSplit(ctx)
	case raftcmdpb.RaftLogGC:
		return d.doExecRaftGC(ctx)
	}

	return nil, nil, nil
}

func (d *applyDelegate) doExecChangePeer(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := new(raftcmdpb.ChangePeerRequest)
	util.MustUnmarshal(req, ctx.req.AdminRequest.Body)

	log.Infof("raftstore-apply[cell-%d]: exec change conf, type=<%s> epoch=<%+v>",
		d.cell.ID,
		req.ChangeType.String(),
		d.cell.Epoch)

	exists := findPeer(&d.cell, req.Peer.StoreID)
	d.cell.Epoch.ConfVer++

	switch req.ChangeType {
	case pdpb.AddNode:
		ctx.metrics.admin.addPeer++

		if exists != nil {
			return nil, nil, nil
		}

		d.cell.Peers = append(d.cell.Peers, &req.Peer)
		log.Infof("raftstore-apply[cell-%d]: peer added, peer=<%+v>",
			d.cell.ID,
			req.Peer)
		ctx.metrics.admin.addPeerSucceed++
	case pdpb.RemoveNode:
		ctx.metrics.admin.removePeer++

		if exists == nil {
			return nil, nil, nil
		}

		// Remove ourself, we will destroy all cell data later.
		// So we need not to apply following logs.
		if d.peerID == req.Peer.ID {
			d.setPendingRemove()
		}

		removePeer(&d.cell, req.Peer.StoreID)
		ctx.metrics.admin.removePeerSucceed++

		// remove pending snapshots
		d.store.trans.forceRemoveSendingSnapshot(req.Peer.ID)

		log.Infof("raftstore-apply[cell-%d]: peer removed, peer=<%+v>",
			d.cell.ID,
			req.Peer)
	}

	state := mraft.Normal

	if d.isPendingRemove() {
		state = mraft.Tombstone
	}

	err := d.ps.updatePeerState(d.cell, state, ctx.wb)
	if err != nil {
		log.Fatalf("raftstore-apply[cell-%d]: update cell state failed, errors:\n %+v",
			d.cell.ID,
			err)
	}

	resp := newAdminRaftCMDResponse(raftcmdpb.ChangePeer, &raftcmdpb.ChangePeerResponse{
		Cell: d.cell,
	})

	result := &execResult{
		adminType: raftcmdpb.ChangePeer,
		// confChange set by applyConfChange
		changePeer: &changePeer{
			peer: req.Peer,
			cell: d.cell,
		},
	}

	return resp, result, nil
}

func (d *applyDelegate) doExecSplit(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	ctx.metrics.admin.split++

	req := new(raftcmdpb.SplitRequest)
	util.MustUnmarshal(req, ctx.req.AdminRequest.Body)

	if len(req.SplitKey) == 0 {
		log.Errorf("raftstore-apply[cell-%d]: missing split key",
			d.cell.ID)
		return nil, nil, errors.New("missing split key")
	}

	req.SplitKey = getOriginKey(req.SplitKey)

	// splitKey < cell.Startkey
	if bytes.Compare(req.SplitKey, d.cell.Start) < 0 {
		log.Errorf("raftstore-apply[cell-%d]: invalid split key, split=<%+v> cell-start=<%+v>",
			d.cell.ID,
			req.SplitKey,
			d.cell.Start)
		return nil, nil, nil
	}

	peer := checkKeyInCell(req.SplitKey, &d.cell)
	if peer != nil {
		log.Errorf("raftstore-apply[cell-%d]: split key not in cell, errors:\n %+v",
			d.cell.ID,
			peer)
		return nil, nil, nil
	}

	if len(req.NewPeerIDs) != len(d.cell.Peers) {
		log.Errorf("raftstore-apply[cell-%d]: invalid new peer id count, splitCount=<%d> currentCount=<%d>",
			d.cell.ID,
			len(req.NewPeerIDs),
			len(d.cell.Peers))

		return nil, nil, nil
	}

	log.Infof("raftstore-apply[cell-%d]: split, splitKey=<%d> cell=<%+v>",
		d.cell.ID,
		req.SplitKey,
		d.cell)

	// After split, the origin cell key range is [start_key, split_key),
	// the new split cell is [split_key, end).
	newCell := metapb.Cell{
		ID:    req.NewCellID,
		Epoch: d.cell.Epoch,
		Start: req.SplitKey,
		End:   d.cell.End,
	}
	d.cell.End = req.SplitKey

	for idx, id := range req.NewPeerIDs {
		newCell.Peers = append(newCell.Peers, &metapb.Peer{
			ID:      id,
			StoreID: d.cell.Peers[idx].StoreID,
		})
	}

	d.cell.Epoch.CellVer++
	newCell.Epoch.CellVer = d.cell.Epoch.CellVer

	err := d.ps.updatePeerState(d.cell, mraft.Normal, ctx.wb)
	if err == nil {
		err = d.ps.updatePeerState(newCell, mraft.Normal, ctx.wb)
	}

	if err == nil {
		err = d.ps.writeInitialState(newCell.ID, ctx.wb)
	}

	if err != nil {
		log.Fatalf("raftstore-apply[cell-%d]: save split cell failed, newCell=<%+v> errors:\n %+v",
			d.cell.ID,
			newCell,
			err)
	}

	rsp := newAdminRaftCMDResponse(raftcmdpb.Split, &raftcmdpb.SplitResponse{
		Left:  d.cell,
		Right: newCell,
	})

	result := &execResult{
		adminType: raftcmdpb.Split,
		splitResult: &splitResult{
			left:  d.cell,
			right: newCell,
		},
	}

	ctx.metrics.admin.splitSucceed++

	return rsp, result, nil
}

func (d *applyDelegate) doExecRaftGC(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	ctx.metrics.admin.compact++

	req := new(raftcmdpb.RaftLogGCRequest)
	util.MustUnmarshal(req, ctx.req.AdminRequest.Body)

	compactIndex := req.CompactIndex
	firstIndex := ctx.applyState.TruncatedState.Index + 1

	if compactIndex <= firstIndex {
		log.Debugf("raftstore-apply[cell-%d]: no need to compact, compactIndex=<%d> firstIndex=<%d>",
			d.cell.ID,
			compactIndex,
			firstIndex)
		return nil, nil, nil
	}

	compactTerm := req.CompactTerm
	if compactTerm == 0 {
		log.Debugf("raftstore-apply[cell-%d]: compact term missing, skip, req=<%+v>",
			d.cell.ID,
			req)
		return nil, nil, errors.New("command format is outdated, please upgrade leader")
	}

	err := compactRaftLog(d.cell.ID, &ctx.applyState, compactIndex, compactTerm)
	if err != nil {
		return nil, nil, err
	}

	rsp := newAdminRaftCMDResponse(raftcmdpb.RaftLogGC, &raftcmdpb.RaftLogGCResponse{})
	result := &execResult{
		adminType: raftcmdpb.RaftLogGC,
		raftGCResult: &raftGCResult{
			state:      ctx.applyState.TruncatedState,
			firstIndex: firstIndex,
		},
	}

	ctx.metrics.admin.compactSucceed++
	return rsp, result, nil
}

func (d *applyDelegate) execWriteRequest(ctx *applyContext) *raftcmdpb.RaftCMDResponse {
	resp := pool.AcquireRaftCMDResponse()
	var err error
	for _, req := range ctx.req.Requests {
		log.Debugf("req: apply raft log. cell=<%d>, uuid=<%d>",
			d.cell.ID,
			req.UUID)

		if h, ok := d.store.redisWriteHandles[req.Type]; ok {
			rsp := h(ctx, req)
			resp.Responses = append(resp.Responses, rsp)
			if err = d.execWriteRequestIndex(req, rsp); err != nil {
				log.Errorf("raftstore-apply[cell-%d]: execWriteRequestIndex failed\n%+v",
					d.cell.ID, err)
			}
		}
	}

	return resp
}

func (d *applyDelegate) execWriteRequestIndex(req *raftcmdpb.Request, rsp *raftcmdpb.Response) (err error) {
	if (req.Type == raftcmdpb.HMSet || req.Type == raftcmdpb.HSet || req.Type == raftcmdpb.Del) && (rsp.ErrorResult == nil && len(rsp.ErrorResults) == 0) {
		cmd := redis.Command(req.Cmd)
		args := cmd.Args()
		end := 1
		if req.Type == raftcmdpb.Del {
			end = len(args)
		}
		listEng := d.store.getListEngine()
		idxReqQueueKey := getIdxReqQueueKey()
		for i := 0; i < end; i++ {
			key := args[i]
			var idxName string
			for name, reExp := range d.store.reExps {
				matched := reExp.Match(key)
				if matched {
					idxName = name
					break
				}
			}
			if idxName != "" {
				idxReq := &pdpb.IndexRequest{}
				idxReq.IdxKey = &pdpb.IndexKeyRequest{
					CellID:   d.cell.ID,
					IdxName:  idxName,
					DataKeys: [][]byte{key},
					IsDel:    false,
				}
				if req.Type == raftcmdpb.Del {
					idxReq.IdxKey.IsDel = true
				}
				var idxReqB []byte
				if idxReqB, err = idxReq.Marshal(); err != nil {
					return
				}
				log.Debugf("raftstore-apply[cell-%d]: will update index %s for key %s",
					d.cell.ID, idxName, key)
				if _, err = listEng.RPush(idxReqQueueKey, idxReqB); err != nil {
					return
				}
			}
		}
	}
	return
}

func (pr *PeerReplicate) doExecReadCmd(c *cmd) {
	resp := pool.AcquireRaftCMDResponse()

	for _, req := range c.req.Requests {
		if h, ok := pr.store.redisReadHandles[req.Type]; ok {
			resp.Responses = append(resp.Responses, h(req))
		}
	}

	c.resp(resp)
}

func newAdminRaftCMDResponse(adminType raftcmdpb.AdminCmdType, subRsp util.Marashal) *raftcmdpb.RaftCMDResponse {
	adminResp := new(raftcmdpb.AdminResponse)
	adminResp.Type = adminType
	adminResp.Body = util.MustMarshal(subRsp)

	resp := pool.AcquireRaftCMDResponse()
	resp.AdminResponse = adminResp

	return resp
}
