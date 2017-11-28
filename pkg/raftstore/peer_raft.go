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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/etcd/raft"
	"github.com/deepfabric/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

var (
	emptyStruct = struct{}{}
)

type requestPolicy int

const (
	readLocal             = requestPolicy(0)
	readIndex             = requestPolicy(1)
	proposeNormal         = requestPolicy(2)
	proposeTransferLeader = requestPolicy(3)
	proposeChange         = requestPolicy(4)

	transferLeaderAllowLogLag = 10

	batch = 1024
)

func (pr *PeerReplicate) onRaftTick(arg interface{}) {
	if !pr.stopRaftTick {
		err := pr.ticks.Put(emptyStruct)
		if err != nil {
			log.Infof("raftstore[cell-%d]: raft tick stopped",
				pr.ps.getCell().ID)
			return
		}

		queueGauge.WithLabelValues(labelQueueTick).Set(float64(pr.ticks.Len()))
		pr.addEvent()
	}

	util.DefaultTimeoutWheel().Schedule(globalCfg.DurationRaftTick, pr.onRaftTick, nil)
}

func (pr *PeerReplicate) addEvent() (bool, error) {
	return pr.events.Offer(emptyStruct)
}

func (pr *PeerReplicate) readyToServeRaft(ctx context.Context) {
	pr.onRaftTick(nil)
	items := make([]interface{}, batch, batch)

	for {
		if pr.events.Len() == 0 && !pr.events.IsDisposed() {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		_, err := pr.events.Get()
		if err != nil {
			pr.metrics.flush()
			pr.actions.Dispose()
			pr.ticks.Dispose()
			pr.steps.Dispose()
			pr.reports.Dispose()

			results := pr.applyResults.Dispose()
			for _, result := range results {
				releaseAsyncApplyResult(result.(*asyncApplyResult))
			}

			// resp all stale requests in batch and queue
			for {
				if pr.batch.isEmpty() {
					break
				}
				c := pr.batch.pop()
				for _, req := range c.req.Requests {
					pr.store.respStoreNotMatch(errStoreNotMatch, req, c.cb)
				}
			}

			requests := pr.requests.Dispose()
			for _, r := range requests {
				req := r.(*reqCtx)
				if req.cb != nil {
					pr.store.respStoreNotMatch(errStoreNotMatch, req.req, req.cb)
				}

				pool.ReleaseRequest(req.req)
				releaseReqCtx(req)
			}

			log.Infof("raftstore[cell-%d]: handle serve raft stopped",
				pr.cellID)
			return
		}

		pr.handleStep(items)
		pr.handleTick(items)
		pr.handleReport(items)
		pr.handleApplyResult(items)
		pr.handleRequest(items)

		if pr.rn.HasReadySince(pr.ps.lastReadyIndex) {
			pr.handleReady()
		}

		pr.handleAction(items)
	}
}

func (pr *PeerReplicate) handleAction(items []interface{}) {
	size := pr.actions.Len()
	if size == 0 {
		return
	}

	n, err := pr.actions.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		a := items[i].(action)
		switch a {
		case checkSplit:
			pr.handleCheckSplit()
		case checkCompact:
			pr.handleCheckCompact()
		}
	}

	if pr.actions.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleCheckCompact() {
	// Leader will replicate the compact log command to followers,
	// If we use current replicated_index (like 10) as the compact index,
	// when we replicate this log, the newest replicated_index will be 11,
	// but we only compact the log to 10, not 11, at that time,
	// the first index is 10, and replicated_index is 11, with an extra log,
	// and we will do compact again with compact index 11, in cycles...
	// So we introduce a threshold, if replicated index - first index > threshold,
	// we will try to compact log.
	// raft log entries[..............................................]
	//                  ^                                       ^
	//                  |-----------------threshold------------ |
	//              first_index                         replicated_index

	var replicatedIdx uint64
	for _, p := range pr.rn.Status().Progress {
		if replicatedIdx == 0 {
			replicatedIdx = p.Match
		}

		if p.Match < replicatedIdx {
			replicatedIdx = p.Match
		}
	}

	// When an election happened or a new peer is added, replicated_idx can be 0.
	if replicatedIdx > 0 {
		lastIdx := pr.rn.LastIndex()
		if lastIdx < replicatedIdx {
			log.Fatalf("raft-log-gc: expect last index >= replicated index, last=<%d> replicated=<%d>",
				lastIdx,
				replicatedIdx)
		}

		raftLogLagHistogram.Observe(float64(lastIdx - replicatedIdx))
	}

	var compactIdx uint64
	appliedIdx := pr.ps.getAppliedIndex()
	firstIdx, _ := pr.ps.FirstIndex()

	if replicatedIdx < firstIdx ||
		replicatedIdx-firstIdx <= globalCfg.ThresholdCompact {
		return
	}

	if appliedIdx > firstIdx &&
		appliedIdx-firstIdx >= globalCfg.LimitCompactCount {
		compactIdx = appliedIdx
	} else if pr.raftLogSizeHint >= globalCfg.LimitCompactBytes {
		compactIdx = appliedIdx
	} else {
		compactIdx = replicatedIdx
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	if compactIdx == 0 {
		log.Fatal("raft-log-gc: unexpect compactIdx")
	}

	// avoid leader send snapshot to the a little lag peer.
	if compactIdx > replicatedIdx {
		if (compactIdx - replicatedIdx) <= globalCfg.LimitCompactLag {
			compactIdx = replicatedIdx
		} else {
			log.Infof("raftstore[cell-%d]: peer lag is too large, maybe sent a snapshot later. lag=<%d>",
				pr.cellID,
				compactIdx-replicatedIdx)
		}
	}

	compactIdx--
	if compactIdx < firstIdx {
		// In case compactIdx == firstIdx before subtraction.
		return
	}

	var gcLogCount uint64
	gcLogCount += compactIdx - firstIdx
	raftLogCompactCounter.Add(float64(gcLogCount))

	term, _ := pr.rn.Term(compactIdx)

	pr.onAdminRequest(newCompactLogRequest(compactIdx, term))
}

func (pr *PeerReplicate) handleCheckSplit() {
	for id, p := range pr.rn.Status().Progress {
		// If a peer is apply snapshot, skip split, avoid sent snapshot again in future.
		if p.State == raft.ProgressStateSnapshot {
			log.Infof("raftstore-split[cell-%d]: peer is applying snapshot",
				pr.cellID,
				id)
			return
		}
	}

	log.Debugf("raftstore-split[cell-%d]: cell need to check whether should split, threshold=<%d> max=<%d>",
		pr.cellID,
		globalCfg.ThresholdSplitCheckBytes,
		pr.sizeDiffHint)

	err := pr.startSplitCheckJob()
	if err != nil {
		log.Errorf("raftstore-split[cell-%d]: add split check job failed, errors:\n %+v",
			pr.cellID,
			err)
		return
	}

	pr.sizeDiffHint = 0
}

func (pr *PeerReplicate) handleTick(items []interface{}) {
	size := pr.ticks.Len()
	if size == 0 {
		return
	}

	n, err := pr.ticks.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		if !pr.stopRaftTick {
			pr.rn.Tick()
		}
	}

	pr.metrics.flush()

	size = pr.ticks.Len()
	queueGauge.WithLabelValues(labelQueueTick).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleStep(items []interface{}) {
	size := pr.steps.Len()
	if size == 0 {
		return
	}

	n, err := pr.steps.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		msg := items[i].(raftpb.Message)
		if pr.isLeader() && msg.From != 0 {
			pr.peerHeartbeatsMap.put(msg.From, time.Now())
		}

		err := pr.rn.Step(msg)
		if err != nil {
			log.Errorf("[cell-%d]: step failed, error:\n%+v",
				pr.cellID,
				err)
		}

		if log.DebugEnabled() {
			if len(msg.Entries) > 0 {
				log.Debugf("req: step raft, cell=<%d>", pr.cellID)
			}
		}
	}

	size = pr.steps.Len()
	queueGauge.WithLabelValues(labelQueueStep).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleReport(items []interface{}) {
	size := pr.reports.Len()
	if size == 0 {
		return
	}

	n, err := pr.reports.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		if msg, ok := items[i].(raftpb.Message); ok {
			pr.rn.ReportUnreachable(msg.To)
			if msg.Type == raftpb.MsgSnap {
				pr.rn.ReportSnapshot(msg.To, raft.SnapshotFailure)
			}
		}
	}

	size = pr.reports.Len()
	queueGauge.WithLabelValues(labelQueueReport).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleApplyResult(items []interface{}) {
	for {
		size := pr.applyResults.Len()
		if size == 0 {
			queueGauge.WithLabelValues(labelQueueApplyResult).Set(float64(0))
			break
		}

		n, err := pr.applyResults.Get(batch, items)
		if err != nil {
			return
		}

		for i := int64(0); i < n; i++ {
			result := items[i].(*asyncApplyResult)
			pr.doPollApply(result)
			releaseAsyncApplyResult(result)
		}

		if n < batch {
			break
		}
	}
}

func (pr *PeerReplicate) handleRequest(items []interface{}) {
	size := pr.requests.Len()
	if size == 0 {
		return
	}

	n, err := pr.requests.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		req := items[i].(*reqCtx)
		pr.batch.push(req)
	}

	for {
		if pr.batch.isEmpty() {
			break
		}

		pr.propose(pr.batch.pop())
	}

	size = pr.requests.Len()
	queueGauge.WithLabelValues(labelQueueReq).Set(float64(0))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleReady() {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if pr.ps.isApplyingSnap() {
		log.Infof("raftstore[cell-%d]: still applying snapshot, skip further handling",
			pr.cellID)
		return
	}

	pr.ps.resetApplyingSnapJob()

	// wait apply committed entries complete
	if pr.rn.HasPendingSnapshot() &&
		!pr.ps.isApplyComplete() {
		log.Debugf("raftstore[cell-%d]: apply index and committed index not match, skip applying snapshot, apply=<%d> commit=<%d>",
			pr.cellID,
			pr.ps.getAppliedIndex(),
			pr.ps.getCommittedIndex())
		return
	}

	rd := pr.rn.ReadySince(pr.ps.lastReadyIndex)
	log.Debugf("raftstore[cell-%d]: raft ready after %d",
		pr.cellID,
		pr.ps.lastReadyIndex)

	ctx := acquireReadyContext()

	// If snapshot is received, further handling
	if !raft.IsEmptySnap(rd.Snapshot) {
		ctx.snap = &mraft.SnapshotMessage{}
		util.MustUnmarshal(ctx.snap, rd.Snapshot.Data)

		if !pr.stopRaftTick {
			// When we apply snapshot, stop raft tick and resume until the snapshot applied
			pr.stopRaftTick = true
		}

		if !pr.store.snapshotManager.Exists(ctx.snap) {
			log.Infof("raftstore[cell-%d]: receiving snapshot, skip further handling",
				pr.cellID)
			return
		}
	}

	ctx.raftState = pr.ps.raftState
	ctx.applyState = pr.ps.applyState
	ctx.lastTerm = pr.ps.lastTerm

	start := time.Now()

	pr.handleRaftReadyAppend(ctx, &rd)
	pr.handleRaftReadyApply(ctx, &rd)

	releaseReadyContext(ctx)

	observeRaftFlowProcessReady(start)
}

func (pr *PeerReplicate) addApplyResult(result *asyncApplyResult) {
	err := pr.applyResults.Put(result)
	if err != nil {
		log.Infof("raftstore[cell-%d]: raft apply result stopped",
			pr.ps.getCell().ID)
		return
	}

	queueGauge.WithLabelValues(labelQueueApplyResult).Set(float64(pr.applyResults.Len()))

	pr.addEvent()
}

func (pr *PeerReplicate) doPollApply(result *asyncApplyResult) {
	pr.doPostApply(result)
	if result.result != nil {
		pr.store.doPostApplyResult(result)
	}
}

func (pr *PeerReplicate) handleRaftReadyAppend(ctx *readyContext, rd *raft.Ready) {
	start := time.Now()

	// If we become leader, send heartbeat to pd
	if rd.SoftState != nil {
		if rd.SoftState.RaftState == raft.StateLeader {
			log.Infof("raftstore[cell-%d]: ********become leader now********",
				pr.cellID)
			pr.doHeartbeat()
			pr.resetBatch()
		}
	}

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if pr.isLeader() {
		pr.send(rd.Messages)
	}

	ctx.wb = pr.store.engine.NewWriteBatch()

	pr.handleAppendSnapshot(ctx, rd)
	pr.handleAppendEntries(ctx, rd)

	if ctx.raftState.LastIndex > 0 && !raft.IsEmptyHardState(rd.HardState) {
		ctx.raftState.HardState = rd.HardState
	}

	pr.handleSaveRaftState(ctx)
	pr.handleSaveApplyState(ctx)

	err := pr.store.engine.Write(ctx.wb)
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors\n %+v",
			pr.getCell().ID,
			err)
	}

	observeRaftLogAppend(start)
}

func (pr *PeerReplicate) handleRaftReadyApply(ctx *readyContext, rd *raft.Ready) {
	if ctx.snap != nil {
		// When apply snapshot, there is no log applied and not compacted yet.
		pr.raftLogSizeHint = 0
	}

	result := pr.doApplySnap(ctx, rd)
	if !pr.isLeader() {
		pr.send(rd.Messages)
	}

	if result != nil {
		pr.startRegistrationJob()
	}

	pr.applyCommittedEntries(rd)

	pr.doApplyReads(rd)

	if result != nil {
		pr.updateKeyRange(result)
	}

	pr.rn.AdvanceAppend(*rd)
	if result != nil {
		// Because we only handle raft ready when not applying snapshot, so following
		// line won't be called twice for the same snapshot.
		pr.rn.AdvanceApply(pr.ps.lastReadyIndex)
	}
}

func (pr *PeerReplicate) handleAppendSnapshot(ctx *readyContext, rd *raft.Ready) {
	if !raft.IsEmptySnap(rd.Snapshot) {
		pr.store.reveivingSnapCount++

		err := pr.getStore().doAppendSnapshot(ctx, rd.Snapshot)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				pr.ps.getCell().ID,
				err)
		}

		pr.metrics.ready.snapshort++
	}
}

func (pr *PeerReplicate) handleAppendEntries(ctx *readyContext, rd *raft.Ready) {
	if len(rd.Entries) > 0 {
		err := pr.getStore().doAppendEntries(ctx, rd.Entries)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				pr.ps.getCell().ID,
				err)
		}

		pr.metrics.ready.append++
	}
}

func (pr *PeerReplicate) handleSaveRaftState(ctx *readyContext) {
	tmp := ctx.raftState
	origin := pr.ps.raftState

	if tmp.LastIndex != origin.LastIndex ||
		tmp.HardState.Commit != origin.HardState.Commit ||
		tmp.HardState.Term != origin.HardState.Term ||
		tmp.HardState.Vote != origin.HardState.Vote {
		err := pr.doSaveRaftState(ctx)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				pr.ps.getCell().ID,
				err)
		}
	}
}

func (pr *PeerReplicate) handleSaveApplyState(ctx *readyContext) {
	tmp := ctx.applyState
	origin := *pr.ps.getApplyState()

	if tmp.AppliedIndex != origin.AppliedIndex ||
		tmp.TruncatedState.Index != origin.TruncatedState.Index ||
		tmp.TruncatedState.Term != origin.TruncatedState.Term {
		err := pr.doSaveApplyState(ctx)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				pr.ps.getCell().ID,
				err)
		}
	}
}

func (pr *PeerReplicate) checkProposal(c *cmd) bool {
	// we handle all read, write and admin cmd here
	if c.req.Header == nil || c.req.Header.UUID == nil {
		c.resp(errorOtherCMDResp(errMissingUUIDCMD))
		return false
	}

	err := pr.store.validateStoreID(c.req)
	if err != nil {
		c.respOtherError(err)
		return false
	}

	term := pr.getCurrentTerm()

	pe := pr.store.validateCell(c.req)
	if err != nil {
		c.resp(errorPbResp(pe, c.req.Header.UUID, term))
		return false
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
	c.term = term
	return true
}

func (pr *PeerReplicate) propose(c *cmd) {
	if !pr.checkProposal(c) {
		return
	}

	if globalCfg.EnableMetricsRequest {
		observeRequestWaitting(c)
	}

	if log.DebugEnabled() {
		for _, req := range c.req.Requests {
			log.Debugf("req: start to proposal. uuid=<%d>",
				req.UUID)
		}
	}

	isConfChange := false
	policy, err := pr.getHandlePolicy(c.req)
	if err != nil {
		resp := errorOtherCMDResp(err)
		c.resp(resp)
		return
	}

	doPropose := false
	switch policy {
	case readLocal:
		pr.execReadLocal(c)
	case readIndex:
		pr.execReadIndex(c)
	case proposeNormal:
		doPropose = pr.proposeNormal(c)
	case proposeTransferLeader:
		doPropose = pr.proposeTransferLeader(c)
	case proposeChange:
		isConfChange = true
		doPropose = pr.proposeConfChange(c)
	}

	if !doPropose {
		return
	}

	err = pr.startProposeJob(c, isConfChange)
	if err != nil {
		c.respOtherError(err)
	}
}

func (pr *PeerReplicate) proposeNormal(c *cmd) bool {
	if !pr.isLeader() {
		c.respNotLeader(pr.cellID, pr.store.getPeer(pr.getLeaderPeerID()))
		return false
	}

	data := util.MustMarshal(c.req)
	size := uint64(len(data))

	raftFlowProposalSizeHistogram.Observe(float64(size))

	if size > globalCfg.LimitRaftEntryBytes {
		c.respLargeRaftEntrySize(pr.cellID, size)
		return false
	}

	idx := pr.nextProposalIndex()
	err := pr.rn.Propose(data)
	if err != nil {
		c.resp(errorOtherCMDResp(err))
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		c.respNotLeader(pr.cellID, pr.store.getPeer(pr.getLeaderPeerID()))
		return false
	}

	if globalCfg.EnableMetricsRequest {
		observeRequestProposal(c)
	}

	pr.metrics.propose.normal++
	return true
}

func (pr *PeerReplicate) proposeConfChange(c *cmd) bool {
	err := pr.checkConfChange(c)
	if err != nil {
		c.respOtherError(err)
		return false
	}

	changePeer := new(raftcmdpb.ChangePeerRequest)
	util.MustUnmarshal(changePeer, c.req.AdminRequest.Body)

	cc := new(raftpb.ConfChange)
	switch changePeer.ChangeType {
	case pdpb.AddNode:
		cc.Type = raftpb.ConfChangeAddNode
	case pdpb.RemoveNode:
		cc.Type = raftpb.ConfChangeRemoveNode
	}
	cc.NodeID = changePeer.Peer.ID
	cc.Context = util.MustMarshal(c.req)

	idx := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(*cc)
	if err != nil {
		c.respOtherError(err)
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		c.respNotLeader(pr.cellID, pr.store.getPeer(pr.getLeaderPeerID()))
		return false
	}

	log.Infof("raftstore[cell-%d]: propose conf change, type=<%s> peer=<%d>",
		pr.cellID,
		changePeer.ChangeType.String(),
		changePeer.Peer.ID)

	pr.metrics.propose.confChange++
	return true
}

func (pr *PeerReplicate) proposeTransferLeader(c *cmd) bool {
	req := new(raftcmdpb.TransferLeaderRequest)
	util.MustUnmarshal(req, c.req.AdminRequest.Body)

	if pr.isTransferLeaderAllowed(&req.Peer) {
		pr.doTransferLeader(&req.Peer)
	} else {
		log.Infof("raftstore[cell-%d]: transfer leader ignored directly, req=<%+v>",
			pr.cellID,
			req)
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	c.resp(newAdminRaftCMDResponse(raftcmdpb.TransferLeader, &raftcmdpb.TransferLeaderResponse{}))
	return false
}

func (pr *PeerReplicate) doTransferLeader(peer *metapb.Peer) {
	log.Infof("raftstore[cell-%d]: transfer leader to %d",
		pr.cellID,
		peer.ID)

	pr.rn.TransferLeader(peer.ID)

	pr.metrics.propose.transferLeader++
}

func (pr *PeerReplicate) isTransferLeaderAllowed(newLeaderPeer *metapb.Peer) bool {
	status := pr.rn.Status()

	if _, ok := status.Progress[newLeaderPeer.ID]; !ok {
		return false
	}

	for _, p := range status.Progress {
		if p.State == raft.ProgressStateSnapshot {
			return false
		}
	}

	lastIndex, _ := pr.ps.LastIndex()

	return lastIndex <= status.Progress[newLeaderPeer.ID].Match+transferLeaderAllowLogLag
}

/// Check whether it's safe to propose the specified conf change request.
/// It's safe iff at least the quorum of the Raft group is still healthy
/// right after that conf change is applied.
/// Define the total number of nodes in current Raft cluster to be `total`.
/// To ensure the above safety, if the cmd is
/// 1. A `AddNode` request
///    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
/// 2. A `RemoveNode` request
///    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
///    need to be up to date for now.
func (pr *PeerReplicate) checkConfChange(c *cmd) error {
	data := c.req.AdminRequest.Body
	changePeer := new(raftcmdpb.ChangePeerRequest)
	util.MustUnmarshal(changePeer, data)

	total := len(pr.rn.Status().Progress)

	if total == 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	peer := changePeer.GetPeer()

	switch changePeer.ChangeType {
	case pdpb.AddNode:
		if _, ok := pr.rn.Status().Progress[peer.ID]; !ok {
			total++
		}
	case pdpb.RemoveNode:
		if _, ok := pr.rn.Status().Progress[peer.ID]; !ok {
			return nil
		}

		total--
	}

	healthy := pr.countHealthyNode()
	quorumAfterChange := total/2 + 1

	if healthy >= quorumAfterChange {
		return nil
	}

	log.Infof("raftstore[cell-%d]: rejects unsafe conf change request, total=<%d> healthy=<%d> quorum after change=<%d>",
		pr.cellID,
		total,
		healthy,
		quorumAfterChange)

	pr.metrics.admin.confChangeReject++
	return fmt.Errorf("unsafe to perform conf change, total=<%d> healthy=<%d> quorum after change=<%d>",
		total,
		healthy,
		quorumAfterChange)
}

/// Count the number of the healthy nodes.
/// A node is healthy when
/// 1. it's the leader of the Raft group, which has the latest logs
/// 2. it's a follower, and it does not lag behind the leader a lot.
///    If a snapshot is involved between it and the Raft leader, it's not healthy since
///    it cannot works as a node in the quorum to receive replicating logs from leader.
func (pr *PeerReplicate) countHealthyNode() int {
	healthy := 0
	for _, p := range pr.rn.Status().Progress {
		if p.Match >= pr.ps.getTruncatedIndex() {
			healthy++
		}
	}

	return healthy
}

func (pr *PeerReplicate) nextProposalIndex() uint64 {
	return pr.rn.NextProposalIndex()
}

func (pr *PeerReplicate) pendingReadCount() int {
	return pr.rn.PendingReadCount()
}

func (pr *PeerReplicate) readyReadCount() int {
	return pr.rn.ReadyReadCount()
}

func (pr *PeerReplicate) isLeader() bool {
	return pr.rn.Status().RaftState == raft.StateLeader
}

func (pr *PeerReplicate) getLeaderPeerID() uint64 {
	return atomic.LoadUint64(&pr.rn.Status().Lead)
}

func (pr *PeerReplicate) send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		err := pr.sendRaftMsg(msg)
		if err != nil {
			// We don't care that the message is sent failed, so here just log this error
			log.Warnf("raftstore[cell-%d]: send msg failure, from_peer=<%d> to_peer=<%d>, errors:\n%s",
				pr.ps.getCell().ID,
				msg.From,
				msg.To,
				err)
		}
		pr.metrics.ready.message++
	}
}

func (pr *PeerReplicate) sendRaftMsg(msg raftpb.Message) error {
	sendMsg := pool.AcquireRaftMessage()
	sendMsg.CellID = pr.ps.getCell().ID
	sendMsg.CellEpoch = pr.ps.getCell().Epoch

	sendMsg.FromPeer = pr.peer
	sendMsg.ToPeer, _ = pr.store.peerCache.get(msg.To)
	if sendMsg.ToPeer.ID == pd.ZeroID {
		return fmt.Errorf("can not found peer<%d>", msg.To)
	}

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	if pr.ps.isInitialized() &&
		(msg.Type == raftpb.MsgVote ||
			// the peer has not been known to this leader, it may exist or not.
			(msg.Type == raftpb.MsgHeartbeat && msg.Commit == 0)) {
		sendMsg.Start = pr.ps.getCell().Start
		sendMsg.End = pr.ps.getCell().End
	}

	sendMsg.Message = msg
	pr.store.trans.sendRaftMessage(sendMsg)

	switch msg.Type {
	case raftpb.MsgApp:
		pr.metrics.message.append++
	case raftpb.MsgAppResp:
		pr.metrics.message.appendResp++
	case raftpb.MsgVote:
		pr.metrics.message.vote++
	case raftpb.MsgVoteResp:
		pr.metrics.message.voteResp++
	case raftpb.MsgSnap:
		pr.rn.ReportSnapshot(msg.To, raft.SnapshotFinish)
		pr.metrics.message.snapshot++
	case raftpb.MsgHeartbeat:
		pr.metrics.message.heartbeat++
	case raftpb.MsgHeartbeatResp:
		pr.metrics.message.heartbeatResp++
	case raftpb.MsgTransferLeader:
		pr.metrics.message.transfeLeader++
	}

	return nil
}

func (pr *PeerReplicate) isRead(req *raftcmdpb.Request) bool {
	_, ok := pr.store.redisReadHandles[req.Type]
	return ok
}

func (pr *PeerReplicate) isWrite(req *raftcmdpb.Request) bool {
	_, ok := pr.store.redisWriteHandles[req.Type]
	return ok
}

func (pr *PeerReplicate) getHandlePolicy(req *raftcmdpb.RaftCMDRequest) (requestPolicy, error) {
	if req.AdminRequest != nil {
		switch req.AdminRequest.Type {
		case raftcmdpb.ChangePeer:
			return proposeChange, nil
		case raftcmdpb.TransferLeader:
			return proposeTransferLeader, nil
		default:
			return proposeNormal, nil
		}
	}

	var isRead, isWrite bool
	for _, r := range req.Requests {
		isRead = pr.isRead(r)
		isWrite = pr.isWrite(r)
	}

	if isRead && isWrite {
		return proposeNormal, errors.New("read and write can't be mixed in one batch")
	}

	if isWrite {
		return proposeNormal, nil
	}

	if req.Header != nil && !req.Header.ReadQuorum {
		return readLocal, nil
	}

	return readIndex, nil
}

func (pr *PeerReplicate) getCurrentTerm() uint64 {
	return pr.rn.Status().Term
}

func (pr *PeerReplicate) step(msg raftpb.Message) {
	err := pr.steps.Put(msg)
	if err != nil {
		log.Infof("raftstore[cell-%d]: raft step stopped",
			pr.ps.getCell().ID)
		return
	}

	queueGauge.WithLabelValues(labelQueueStep).Set(float64(pr.steps.Len()))

	pr.addEvent()
}

func (pr *PeerReplicate) report(report interface{}) {
	err := pr.reports.Put(report)
	if err != nil {
		log.Infof("raftstore[cell-%d]: raft report stopped",
			pr.ps.getCell().ID)
		return
	}

	queueGauge.WithLabelValues(labelQueueReport).Set(float64(pr.reports.Len()))

	pr.addEvent()
}

func getRaftConfig(id, appliedIndex uint64, store raft.Storage) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    globalCfg.ThresholdRaftElection,
		HeartbeatTick:   globalCfg.ThresholdRaftHeartbeat,
		MaxSizePerMsg:   globalCfg.LimitRaftMsgBytes,
		MaxInflightMsgs: globalCfg.LimitRaftMsgCount,
		Storage:         store,
		CheckQuorum:     true,
		PreVote:         false,
	}
}
