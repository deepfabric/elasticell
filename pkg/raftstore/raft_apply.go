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
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/etcd/raft/raftpb"
)

type applyMetrics struct {
	// an inaccurate difference in cell size since last reset.
	sizeDiffHint int64
	// delete keys' count since last reset.
	deleteKeysHint uint64
	writtenBytes   int64
	writtenKeys    uint64

	admin raftAdminMetrics
}

type asyncApplyResult struct {
	cellID           uint64
	appliedIndexTerm uint64
	applyState       mraft.RaftApplyState
	result           *execResult
	metrics          applyMetrics
}

func (res *asyncApplyResult) reset() {
	res.cellID = 0
	res.appliedIndexTerm = 0
	res.applyState = emptyApplyState
	res.result = nil
	res.metrics = emptyApplyMetrics
}

func (res *asyncApplyResult) hasSplitExecResult() bool {
	return nil != res.result && res.result.splitResult != nil
}

type changePeer struct {
	confChange raftpb.ConfChange
	peer       metapb.Peer
	cell       metapb.Cell
}

type splitResult struct {
	left  metapb.Cell
	right metapb.Cell
}

type raftGCResult struct {
	state      mraft.RaftTruncatedState
	firstIndex uint64
}

type execResult struct {
	adminType    raftcmdpb.AdminCmdType
	changePeer   *changePeer
	splitResult  *splitResult
	raftGCResult *raftGCResult
}

type applyDelegate struct {
	sync.RWMutex

	store *Store
	ps    *peerStorage

	peerID uint64
	cell   metapb.Cell

	// if we remove ourself in ChangePeer remove, we should set this flag, then
	// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	applyState       mraft.RaftApplyState
	appliedIndexTerm uint64
	term             uint64

	pendingCMDs          []*cmd
	pendingChangePeerCMD *cmd
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	d.Lock()
	for _, c := range d.pendingCMDs {
		d.notifyStaleCMD(c)
	}

	if nil != d.pendingChangePeerCMD {
		d.notifyStaleCMD(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = make([]*cmd, 0)
	d.pendingChangePeerCMD = nil
	d.Unlock()
}

func (d *applyDelegate) findCB(ctx *applyContext) *cmd {
	if isChangePeerCMD(ctx.req) {
		c := d.getPendingChangePeerCMD()
		if c == nil || c.req == nil {
			return nil
		} else if bytes.Compare(ctx.req.Header.UUID, c.getUUID()) == 0 {
			return c
		}

		d.notifyStaleCMD(c)
		return nil
	}

	for {
		head := d.popPendingCMD(ctx.term)
		if head == nil {
			return nil
		}

		if bytes.Compare(head.getUUID(), ctx.req.Header.UUID) == 0 {
			return head
		}

		if log.DebugEnabled() {
			log.Debugf("raftstore-apply[cell-%d]: notify stale cmd, cmd=<%+v>",
				d.cell.ID,
				head)
		}

		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		d.notifyStaleCMD(head)
	}
}

func (d *applyDelegate) appendPendingCmd(c *cmd) {
	d.pendingCMDs = append(d.pendingCMDs, c)
}

func (d *applyDelegate) setPendingChangePeerCMD(c *cmd) {
	d.Lock()
	d.pendingChangePeerCMD = c
	d.Unlock()
}

func (d *applyDelegate) getPendingChangePeerCMD() *cmd {
	d.RLock()
	c := d.pendingChangePeerCMD
	d.RUnlock()

	return c
}

func (d *applyDelegate) popPendingCMD(raftLogEntryTerm uint64) *cmd {
	d.Lock()
	if len(d.pendingCMDs) == 0 {
		d.Unlock()
		return nil
	}

	if d.pendingCMDs[0].term > raftLogEntryTerm {
		d.Unlock()
		return nil
	}

	c := d.pendingCMDs[0]
	d.pendingCMDs[0] = nil
	d.pendingCMDs = d.pendingCMDs[1:]
	d.Unlock()
	return c
}

func isChangePeerCMD(req *raftcmdpb.RaftCMDRequest) bool {
	return nil != req.AdminRequest &&
		req.AdminRequest.Type == raftcmdpb.ChangePeer
}

func (d *applyDelegate) notifyStaleCMD(c *cmd) {
	log.Debugf("raftstore-apply[cell-%d]: resp stale, cmd=<%+v>, current=<%d>",
		d.cell.ID,
		c,
		d.term)
	resp := errorStaleCMDResp(c.getUUID(), d.term)
	c.resp(resp)
}

func (d *applyDelegate) notifyCellRemoved(c *cmd) {
	log.Infof("raftstore-destroy[cell-%d]: cmd is removed, skip. cmd=<%+v>",
		d.cell.ID,
		c)
	c.respCellNotFound(d.cell.ID)
}

func (d *applyDelegate) applyCommittedEntries(commitedEntries []raftpb.Entry) {
	if len(commitedEntries) <= 0 {
		return
	}

	start := time.Now()
	ctx := acquireApplyContext()
	req := pool.AcquireRaftCMDRequest()

	for idx, entry := range commitedEntries {
		if d.isPendingRemove() {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectIndex := d.applyState.AppliedIndex + 1
		if expectIndex != entry.Index {
			log.Fatalf("raftstore-apply[cell-%d]: index not match, expect=<%d> get=<%d> state=<%+v> entry=<%+v>",
				d.cell.ID,
				expectIndex,
				entry.Index,
				d.applyState,
				entry)
		}

		if idx > 0 {
			ctx.reset()
			req.Reset()
		}

		ctx.req = req
		ctx.applyState = d.applyState
		ctx.index = entry.Index
		ctx.term = entry.Term

		var result *execResult

		switch entry.Type {
		case raftpb.EntryNormal:
			result = d.applyEntry(ctx, &entry)
		case raftpb.EntryConfChange:
			result = d.applyConfChange(ctx, &entry)
		}

		asyncResult := acquireAsyncApplyResult()

		asyncResult.cellID = d.cell.ID
		asyncResult.appliedIndexTerm = d.appliedIndexTerm
		asyncResult.applyState = d.applyState
		asyncResult.result = result

		if ctx != nil {
			asyncResult.metrics = ctx.metrics
		}

		pr := d.store.replicatesMap.get(d.cell.ID)
		if pr != nil {
			pr.addApplyResult(asyncResult)
		}
	}

	// only release RaftCMDRequest. Header and Requests fields is pb created in Unmarshal
	pool.ReleaseRaftCMDRequest(req)
	releaseApplyContext(ctx)

	observeRaftLogApply(start)
}

func (d *applyDelegate) applyEntry(ctx *applyContext, entry *raftpb.Entry) *execResult {
	if len(entry.Data) > 0 {
		util.MustUnmarshal(ctx.req, entry.Data)
		return d.doApplyRaftCMD(ctx)
	}

	// when a peer become leader, it will send an empty entry.
	state := d.applyState
	state.AppliedIndex = entry.Index

	err := d.store.getMetaEngine().Set(getApplyStateKey(d.cell.ID), util.MustMarshal(&state))
	if err != nil {
		log.Fatalf("raftstore-apply[cell-%d]: apply empty entry failed, entry=<%s> errors:\n %+v",
			d.cell.ID,
			entry.String(),
			err)
	}

	d.applyState.AppliedIndex = entry.Index
	d.appliedIndexTerm = entry.Term
	if entry.Term <= 0 {
		panic("error empty entry term.")
	}

	for {
		c := d.popPendingCMD(entry.Term - 1)
		if c == nil {
			return nil
		}

		// apprently, all the callbacks whose term is less than entry's term are stale.
		d.notifyStaleCMD(c)
	}
}

func (d *applyDelegate) applyConfChange(ctx *applyContext, entry *raftpb.Entry) *execResult {
	cc := new(raftpb.ConfChange)

	util.MustUnmarshal(cc, entry.Data)
	util.MustUnmarshal(ctx.req, cc.Context)

	result := d.doApplyRaftCMD(ctx)
	if nil == result {
		return &execResult{
			adminType:  raftcmdpb.ChangePeer,
			changePeer: &changePeer{},
		}
	}

	result.changePeer.confChange = *cc
	return result
}

func (d *applyDelegate) destroy() {
	d.Lock()
	for _, c := range d.pendingCMDs {
		d.notifyCellRemoved(c)
	}

	if d.pendingChangePeerCMD != nil && d.pendingChangePeerCMD.req != nil {
		d.notifyCellRemoved(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = nil
	d.pendingChangePeerCMD = nil
	d.Unlock()
}

func (d *applyDelegate) setPendingRemove() {
	d.Lock()
	d.pendingRemove = true
	d.Unlock()
}

func (d *applyDelegate) isPendingRemove() bool {
	d.RLock()
	value := d.pendingRemove
	d.RUnlock()

	return value
}
