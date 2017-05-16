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
	"github.com/deepfabric/elasticell/pkg/redis"
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

	// TODO: imple write command
	for _, req := range ctx.req.Requests {
		switch req.Type {
		case raftcmdpb.Set:
			resp.Responses = append(resp.Responses, d.execKVSet(req))
		case raftcmdpb.Incrby:
			resp.Responses = append(resp.Responses, d.execKVIncrBy(req))
		case raftcmdpb.Incr:
			resp.Responses = append(resp.Responses, d.execKVIncr(req))
		case raftcmdpb.Decrby:
			resp.Responses = append(resp.Responses, d.execKVDecrby(req))
		case raftcmdpb.Decr:
			resp.Responses = append(resp.Responses, d.execKVDecr(req))
		case raftcmdpb.GetSet:
			resp.Responses = append(resp.Responses, d.execKVGetSet(req))
		case raftcmdpb.Append:
			resp.Responses = append(resp.Responses, d.execKVAppend(req))
		case raftcmdpb.Setnx:
			resp.Responses = append(resp.Responses, d.execKVSetNX(req))
		case raftcmdpb.HSet:
			resp.Responses = append(resp.Responses, d.execHSet(req))
		case raftcmdpb.HDel:
			resp.Responses = append(resp.Responses, d.execHDel(req))
		case raftcmdpb.HMSet:
			resp.Responses = append(resp.Responses, d.execHMSet(req))
		case raftcmdpb.HSetNX:
			resp.Responses = append(resp.Responses, d.execHSetNX(req))
		case raftcmdpb.HIncrBy:
			resp.Responses = append(resp.Responses, d.execHIncrBy(req))
		case raftcmdpb.LInsert:
			resp.Responses = append(resp.Responses, d.execLInsert(req))
		case raftcmdpb.LPop:
			resp.Responses = append(resp.Responses, d.execLPop(req))
		case raftcmdpb.LPush:
			resp.Responses = append(resp.Responses, d.execLPush(req))
		case raftcmdpb.LPushX:
			resp.Responses = append(resp.Responses, d.execLPushX(req))
		case raftcmdpb.LRem:
			resp.Responses = append(resp.Responses, d.execLRem(req))
		case raftcmdpb.LSet:
			resp.Responses = append(resp.Responses, d.execLSet(req))
		case raftcmdpb.LTrim:
			resp.Responses = append(resp.Responses, d.execLTrim(req))
		case raftcmdpb.RPop:
			resp.Responses = append(resp.Responses, d.execRPop(req))
		case raftcmdpb.RPush:
			resp.Responses = append(resp.Responses, d.execRPush(req))
		case raftcmdpb.RPushX:
			resp.Responses = append(resp.Responses, d.execRPushX(req))
		}
	}

	return nil
}

func (pr *PeerReplicate) doExecReadCmd(cmd *cmd) {
	resp := new(raftcmdpb.RaftCMDResponse)

	// TODO: impl read cmd
	for _, req := range cmd.req.Requests {
		switch req.Type {
		case raftcmdpb.Get:
			resp.Responses = append(resp.Responses, pr.execKVGet(req))
		case raftcmdpb.StrLen:
			resp.Responses = append(resp.Responses, pr.execKVStrLen(req))
		case raftcmdpb.HGet:
			resp.Responses = append(resp.Responses, pr.execHGet(req))
		case raftcmdpb.HExists:
			resp.Responses = append(resp.Responses, pr.execHExists(req))
		case raftcmdpb.HKeys:
			resp.Responses = append(resp.Responses, pr.execHKeys(req))
		case raftcmdpb.HVals:
			resp.Responses = append(resp.Responses, pr.execHVals(req))
		case raftcmdpb.HGetAll:
			resp.Responses = append(resp.Responses, pr.execHGetAll(req))
		case raftcmdpb.HLen:
			resp.Responses = append(resp.Responses, pr.execHLen(req))
		case raftcmdpb.HMGet:
			resp.Responses = append(resp.Responses, pr.execHMGet(req))
		case raftcmdpb.HStrLen:
			resp.Responses = append(resp.Responses, pr.execHStrLen(req))
		case raftcmdpb.LIndex:
			resp.Responses = append(resp.Responses, pr.execLIndex(req))
		case raftcmdpb.LLEN:
			resp.Responses = append(resp.Responses, pr.execLLEN(req))
		case raftcmdpb.LRange:
			resp.Responses = append(resp.Responses, pr.execLRange(req))
		}
	}
}

// kv
func (d *applyDelegate) execKVSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	err := d.store.getKVEngine().Set(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (pr *PeerReplicate) execKVGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getKVEngine().Get(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		BulkResult: value,
	}
}

func (pr *PeerReplicate) execKVStrLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	n, err := pr.store.getKVEngine().StrLen(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (pr *PeerReplicate) execHGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getHashEngine().HGet(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (pr *PeerReplicate) execHExists(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	exists, err := pr.store.getHashEngine().HExists(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var value int64
	if exists {
		value = 1
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (pr *PeerReplicate) execHKeys(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getHashEngine().HKeys(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var has = true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (pr *PeerReplicate) execHVals(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getHashEngine().HVals(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var has = true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (pr *PeerReplicate) execHGetAll(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getHashEngine().HGetAll(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	var has = true
	return &raftcmdpb.Response{
		FvPairArrayResult:         value,
		HasEmptyFVPairArrayResult: &has,
	}
}

func (pr *PeerReplicate) execHLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getHashEngine().HLen(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (pr *PeerReplicate) execHMGet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, errs := pr.store.getHashEngine().HMGet(args[0], args[1:]...)
	if errs != nil {
		errors := make([][]byte, len(errs))
		for idx, err := range errs {
			errors[idx] = util.StringToSlice(err.Error())
		}

		return &raftcmdpb.Response{
			ErrorResults: errors,
		}
	}

	has := true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (pr *PeerReplicate) execHStrLen(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getHashEngine().HStrLen(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (pr *PeerReplicate) execLIndex(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	index, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := pr.store.getListEngine().LIndex(args[0], index)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (pr *PeerReplicate) execLLEN(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := pr.store.getListEngine().LLen(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (pr *PeerReplicate) execLRange(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	start, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	end, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := pr.store.getListEngine().LRange(args[0], start, end)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (d *applyDelegate) execKVIncrBy(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	incrment, err := util.StrInt64(args[1])
	if err != nil {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getKVEngine().IncrBy(args[0], incrment)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execKVIncr(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getKVEngine().IncrBy(args[0], 1)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execKVDecrby(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	incrment, err := util.StrInt64(args[1])
	if err != nil {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getKVEngine().DecrBy(args[0], incrment)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execKVDecr(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getKVEngine().DecrBy(args[0], 1)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execKVGetSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getKVEngine().GetSet(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		BulkResult: value,
	}
}

func (d *applyDelegate) execKVAppend(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getKVEngine().Append(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execKVSetNX(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getKVEngine().SetNX(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execHSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getHashEngine().HSet(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execHDel(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	n, err := d.store.getHashEngine().HDel(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &n,
	}
}

func (d *applyDelegate) execHMSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	l := len(args)
	if l < 3 || l%2 == 0 {
		return redis.ErrInvalidCommandResp
	}

	key := args[0]
	kvs := args[1:]
	l = len(kvs) / 2
	fields := make([][]byte, l)
	values := make([][]byte, l)

	for i := 0; i < len(kvs); i++ {
		fields[i] = kvs[2*i]
		values[i] = kvs[2*i+1]
	}

	err := d.store.getHashEngine().HMSet(key, fields, values)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (d *applyDelegate) execHSetNX(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getHashEngine().HSetNX(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (d *applyDelegate) execHIncrBy(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	incrment, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := d.store.getHashEngine().HIncrBy(args[0], args[1], incrment)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	v, err := util.StrInt64(value)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}
	return &raftcmdpb.Response{
		IntegerResult: &v,
	}
}

func (d *applyDelegate) execLInsert(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 4 {
		return redis.ErrInvalidCommandResp
	}

	pos, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := d.store.getListEngine().LInsert(args[0], int(pos), args[2], args[3])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (d *applyDelegate) execLPop(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getListEngine().LPop(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (d *applyDelegate) execLPush(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getListEngine().LPush(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (d *applyDelegate) execLPushX(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getListEngine().LPushX(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (d *applyDelegate) execLRem(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	count, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := d.store.getListEngine().LRem(args[0], count, args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (d *applyDelegate) execLSet(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	index, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	err = d.store.getListEngine().LSet(args[0], index, args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (d *applyDelegate) execLTrim(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return redis.ErrInvalidCommandResp
	}

	begin, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	end, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	err = d.store.getListEngine().LTrim(args[0], begin, end)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return redis.OKStatusResp
}

func (d *applyDelegate) execRPop(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getListEngine().RPop(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}

func (d *applyDelegate) execRPush(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getListEngine().RPush(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (d *applyDelegate) execRPushX(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := d.store.getListEngine().RPushX(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func newAdminRaftCMDResponse(adminType raftcmdpb.AdminCmdType, subRsp util.Marashal) *raftcmdpb.RaftCMDResponse {
	adminResp := new(raftcmdpb.AdminResponse)
	adminResp.Type = adminType
	adminResp.Body = util.MustMarshal(subRsp)

	resp := new(raftcmdpb.RaftCMDResponse)
	resp.AdminResponse = adminResp

	return resp
}
