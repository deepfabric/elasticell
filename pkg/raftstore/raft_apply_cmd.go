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
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
)

type execContext struct {
	wb         storage.WriteBatch
	applyState mraft.RaftApplyState
	req        *raftcmdpb.RaftCMDRequest
	index      uint64
	term       uint64
}

func (d *applyDelegate) checkEpoch(req *raftcmdpb.RaftCMDRequest) bool {
	return checkEpoch(d.cell, req)
}

func (d *applyDelegate) doApplyRaftCMD(req *raftcmdpb.RaftCMDRequest, term uint64, index uint64) *execResult {
	if index == 0 {
		log.Fatalf("raftstore-apply[cell-%d]: apply raft command needs a none zero index",
			d.cell.ID)
	}

	cb := d.findCB(req.Header.UUID, term, req)

	if d.isPendingRemove() {
		log.Fatalf("raftstore-apply[cell-%d]: apply raft comand can not pending remove",
			d.cell.ID)
	}

	var err error
	var resp *raftcmdpb.RaftCMDResponse
	var result *execResult
	ctx := &execContext{
		applyState: d.applyState,
		req:        req,
		index:      index,
		term:       term,
	}

	if !d.checkEpoch(req) {
		resp = errorStaleEpochResp(req.Header.UUID, d.term, d.cell)
	} else {
		if req.AdminRequest != nil {
			resp, result, err = d.execAdminRequest(ctx)
			if err != nil {
				resp = errorStaleEpochResp(req.Header.UUID, d.term, d.cell)
			}
		} else {
			resp = d.execWriteRequest(ctx)
		}
	}

	ctx.applyState.AppliedIndex = index
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
	d.term = term

	log.Debugf("raftstore-apply[cell-%d]: applied command, uuid=<%v> index=<%d>",
		d.cell.ID,
		req.Header.UUID,
		index)

	if cb != nil {
		buildTerm(d.term, resp)
		buildUUID(req.Header.UUID, resp)

		// resp client
		cb(resp)
	}

	return result
}

func (d *applyDelegate) execAdminRequest(ctx *execContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
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

func (d *applyDelegate) doExecChangePeer(ctx *execContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := new(raftcmdpb.ChangePeerRequest)
	util.MustUnmarshal(req, ctx.req.AdminRequest.Body)

	log.Infof("raftstore-apply[cell-%d]: exec change conf, type=<%s> epoch=<%+v>",
		d.cell.ID,
		req.ChangeType.String(),
		d.cell.Epoch)

	exists := findPeer(&d.cell, req.Peer.StoreID)

	switch req.ChangeType {
	case pdpb.AddNode:
		if exists != nil {
			return nil, nil, nil
		}

		d.cell.Peers = append(d.cell.Peers, &req.Peer)
		log.Infof("raftstore-apply[cell-%d]: peer added, peer=<%+v>",
			d.cell.ID,
			req.Peer)
	case pdpb.RemoveNode:
		if exists == nil {
			return nil, nil, nil
		}

		// Remove ourself, we will destroy all cell data later.
		// So we need not to apply following logs.
		if d.peerID == req.Peer.ID {
			d.pendingRemove = true
		}

		removePeer(&d.cell, req.Peer.StoreID)
		log.Infof("raftstore-apply[cell-%d]: peer removed, peer=<%+v>",
			d.cell.ID,
			req.Peer)
	}

	state := mraft.Normal

	if d.pendingRemove {
		state = mraft.Tombstone
	}

	err := d.ps.updatePeerState(d.cell, state, ctx.wb)
	if err != nil {
		log.Fatalf("raftstore-apply[cell-%d]: update cell state failed, errors:\n %+v",
			d.cell.ID,
			err)
	}

	d.cell.Epoch.ConfVer++

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

func (d *applyDelegate) doExecSplit(ctx *execContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := new(raftcmdpb.SplitRequest)
	util.MustUnmarshal(req, ctx.req.AdminRequest.Body)

	if len(req.SplitKey) == 0 {
		log.Errorf("raftstore-apply[cell-%d]: missing split key",
			d.cell.ID)
		return nil, nil, errors.New("missing split key")
	}

	// splitKey < cell.Startkey
	if bytes.Compare(req.SplitKey, d.cell.Start) < 0 {
		log.Errorf("raftstore-apply[cell-%d]: invalid split key, split=<%+v> cell-start=<%+v>",
			d.cell.ID,
			req.SplitKey,
			d.cell.Start)
		return nil, nil, nil
	}

	perr := checkKeyInCell(req.SplitKey, &d.cell)
	if perr != nil {
		log.Errorf("raftstore-apply[cell-%d]: split key not in cell, errors:\n %+v",
			d.cell.ID,
			perr)
		return nil, nil, nil
	}

	if len(req.NewPeerIDs) != len(d.cell.Peers) {
		log.Errorf("raftstore-apply[cell-%d]: invalid new peer id count, splitCount=<%d> currentCount=<%d>",
			d.cell.ID,
			len(req.NewPeerIDs),
			len(d.cell.Peers))

		return nil, nil, nil
	}

	log.Infof("raftstore-apply[cell-%d]: split, splitKey=<%+v> cell=<%+v>",
		d.cell.ID,
		req.SplitKey,
		d.cell)

	// After split, the origin cell key range is [start_key, split_key),
	// the new split cell is [split_key, end).
	newCell := d.cell
	newCell.Peers = make([]*metapb.Peer, len(req.NewPeerIDs))

	newCell.Start = req.SplitKey
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
	if err != nil {
		err = d.ps.updatePeerState(newCell, mraft.Normal, ctx.wb)
	}

	if err != nil {
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

	return rsp, result, nil
}

func (d *applyDelegate) doExecRaftGC(ctx *execContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
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

	return rsp, result, nil
}

func (d *applyDelegate) execWriteRequest(ctx *execContext) *raftcmdpb.RaftCMDResponse {
	resp := new(raftcmdpb.RaftCMDResponse)

	for _, req := range ctx.req.Requests {
		if h, ok := d.store.redisWriteHandles[req.Type]; ok {
			resp.Responses = append(resp.Responses, h(req))
		}
	}

	return resp
}

func (pr *PeerReplicate) doExecReadCmd(cmd *cmd) {
	resp := new(raftcmdpb.RaftCMDResponse)

	for _, req := range cmd.req.Requests {
		if h, ok := pr.store.redisReadHandles[req.Type]; ok {
			resp.Responses = append(resp.Responses, h(req))
		}
	}

	cmd.resp(resp)
}

func newAdminRaftCMDResponse(adminType raftcmdpb.AdminCmdType, subRsp util.Marashal) *raftcmdpb.RaftCMDResponse {
	adminResp := new(raftcmdpb.AdminResponse)
	adminResp.Type = adminType
	adminResp.Body = util.MustMarshal(subRsp)

	resp := new(raftcmdpb.RaftCMDResponse)
	resp.AdminResponse = adminResp

	return resp
}
