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
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
)

type execContext struct {
	applyState mraft.RaftApplyState
	req        *raftcmdpb.RaftCMDRequest
	index      uint64
	term       uint64
}

func (d *applyDelegate) checkEpoch(req *raftcmdpb.RaftCMDRequest) bool {
	checkVer := false
	checkConfVer := false

	if req.AdminRequest != nil {
		switch req.AdminRequest.Type {
		case raftcmdpb.Split:
			checkVer = true
		case raftcmdpb.ChangePeer:
			checkConfVer = true
		case raftcmdpb.TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	} else {
		// for redis command, we don't care conf version.
		checkConfVer = true
	}

	if !checkConfVer && !checkVer {
		return true
	}

	if req.Header == nil {
		return false
	}

	fromEpoch := req.Header.CellEpoch
	lastestEpoch := d.cell.Epoch

	if (checkConfVer && fromEpoch.ConfVer < lastestEpoch.ConfVer) ||
		(checkVer && fromEpoch.CellVer < lastestEpoch.CellVer) {
		log.Infof("raftstore-apply[cell-%d]: reveiced stale epoch, lastest=<%s> reveived=<%s>",
			d.cell.ID,
			lastestEpoch.String(),
			fromEpoch.String())
		return false
	}

	return true
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
		// TODO: use write batch
		if req.AdminRequest != nil {
			resp, result = d.execAdminRequest(ctx)
		} else {
			resp = d.execWriteRequest(ctx)
		}
	}

	ctx.applyState.AppliedIndex = index
	if !d.isPendingRemove() {
		// TODO: use write batch
		err := d.store.engine.Set(getApplyStateKey(d.cell.ID), util.MustMarshal(&ctx.applyState))
		if err != nil {
			log.Fatalf("raftstore-apply[cell-%d]: save apply context failed, errors:\n %+v",
				d.cell.ID,
				err)
		}
	}
	// TODO: batch commit, use write batch.
	d.applyState = ctx.applyState
	d.term = term

	// TODO: impl
	// if let Some(ref exec_result) = exec_result {
	//         match *exec_result {
	//             ExecResult::ChangePeer(ref cp) => {
	//                 self.region = cp.region.clone();
	//             }
	//             ExecResult::ComputeHash { .. } |
	//             ExecResult::VerifyHash { .. } |
	//             ExecResult::CompactLog { .. } => {}
	//             ExecResult::SplitRegion { ref left, .. } => {
	//                 self.region = left.clone();
	//                 self.metrics.size_diff_hint = 0;
	//                 self.metrics.delete_keys_hint = 0;
	//             }
	//         }
	//     }

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

func (d *applyDelegate) execAdminRequest(ctx *execContext) (*raftcmdpb.RaftCMDResponse, *execResult) {
	// TODO: impl
	return nil, nil
}

func (d *applyDelegate) execWriteRequest(ctx *execContext) *raftcmdpb.RaftCMDResponse {
	for _, req := range ctx.req.Requests {
		switch req.Type {
		// TODO: imple write command
		}
	}

	return nil
}

func (pr *PeerReplicate) execReadRequest(req *readIndexRequest) {
	for _, cmd := range req.cmds {
		pr.doExecReadCmd(cmd)
	}
}

func (pr *PeerReplicate) doExecReadCmd(cmd *cmd) {
	// TODO: imple read cmd
}
