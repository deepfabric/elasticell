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
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
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

	eventBatch = 1024
)

type proposalMeta struct {
	cmd  *cmd
	term uint64
}

func (pr *PeerReplicate) onRaftTick(arg interface{}) {
	err := pr.ticks.Put(emptyStruct)
	if err != nil {
		log.Infof("raftstore[cell-%d]: raft tick stopped",
			pr.ps.getCell().ID)
		return
	}

	queueGauge.WithLabelValues(labelQueueTick).Set(float64(pr.ticks.Len()))
	util.DefaultTimeoutWheel().Schedule(globalCfg.getRaftBaseTickDuration(), pr.onRaftTick, nil)

	pr.addEvent()
}

func (pr *PeerReplicate) addEvent() {
	pr.events.Put(emptyStruct)
}

func (pr *PeerReplicate) readyToServeRaft(ctx context.Context) {
	pr.onRaftTick(nil)

	for {
		_, err := pr.events.Get(eventBatch)
		if err != nil {
			pr.metrics.flush()

			pr.ticks.Dispose()
			pr.steps.Dispose()
			pr.reports.Dispose()
			pr.applyResults.Dispose()
			pr.proposes.Dispose()

			// resp all stale requests
			requests := pr.requests.Dispose()
			for _, r := range requests {
				req := r.(*reqCtx)
				if req.cb != nil {
					pr.store.respStoreNotMatch(errStoreNotMatch, req.req, req.cb)
				}
			}

			log.Infof("raftstore[cell-%d]: handle serve raft stopped",
				pr.cellID)
			return
		}

		pr.handleTick()
		pr.handleStep()
		pr.handleReport()
		pr.handleApplyResult()
		pr.handleRequest()
		pr.handlePropose()

		if pr.rn.HasReadySince(pr.ps.lastReadyIndex) {
			pr.handleReady()
		}
	}
}

func (pr *PeerReplicate) handleTick() {
	size := pr.ticks.Len()
	ticks, err := pr.ticks.Get(size)
	if err != nil {
		return
	}

	for _ = range ticks {
		pr.rn.Tick()
	}

	pr.metrics.flush()

	size = pr.ticks.Len()
	queueGauge.WithLabelValues(labelQueueTick).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleStep() {
	size := pr.steps.Len()
	msgs, err := pr.steps.Get(size)
	if err != nil {
		return
	}

	for _, m := range msgs {
		msg := m.(raftpb.Message)
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

func (pr *PeerReplicate) handleReport() {
	size := pr.reports.Len()
	msgs, err := pr.reports.Get(size)
	if err != nil {
		return
	}

	for _, m := range msgs {
		msg := m.(raftpb.Message)
		pr.rn.ReportUnreachable(msg.To)
		if msg.Type == raftpb.MsgSnap {
			pr.rn.ReportSnapshot(msg.To, raft.SnapshotFailure)
		}
	}

	size = pr.reports.Len()
	queueGauge.WithLabelValues(labelQueueReport).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleApplyResult() {
	size := pr.applyResults.Len()
	results, err := pr.applyResults.Get(size)
	if err != nil {
		return
	}

	for _, r := range results {
		result := r.(*asyncApplyResult)
		pr.doPollApply(result)
	}

	if size > 0 {
		pr.nextBatch()
	}

	size = pr.applyResults.Len()
	queueGauge.WithLabelValues(labelQueueApplyResult).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleRequest() {
	size := pr.requests.Len()
	requests, err := pr.requests.Get(size)
	if err != nil {
		return
	}

	for _, r := range requests {
		req := r.(*reqCtx)
		pr.batch.push(req)
	}

	if size > 0 && pr.batch.isLastComplete() {
		pr.nextBatch()
	}

	size = pr.requests.Len()
	queueGauge.WithLabelValues(labelQueueReq).Set(float64(size))

	if size > 0 {
		pr.addEvent()
	}

}

func (pr *PeerReplicate) handlePropose() {
	size := pr.proposes.Len()
	proposes, err := pr.proposes.Get(size)
	if err != nil {
		return
	}

	complete := 0
	for _, p := range proposes {
		meta := p.(*proposalMeta)
		if pr.propose(meta) {
			complete++
		}
	}

	if complete > 0 {
		pr.nextBatch()
	}

	size = pr.proposes.Len()
	queueGauge.WithLabelValues(labelQueuePropose).Set(float64(size))

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

	if pr.rn.HasReadySince(pr.ps.lastReadyIndex) {
		rd := pr.rn.ReadySince(pr.ps.lastReadyIndex)

		log.Debugf("raftstore[cell-%d]: raft ready after %d",
			pr.cellID,
			pr.ps.lastReadyIndex)

		ctx := &tempRaftContext{
			raftState:  pr.ps.raftState,
			applyState: pr.ps.applyState,
			lastTerm:   pr.ps.lastTerm,
		}

		start := time.Now()

		pr.handleRaftReadyAppend(ctx, &rd)
		pr.handleRaftReadyApply(ctx, &rd)

		observeRaftFlowProcessReady(start)
	}
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

func (pr *PeerReplicate) handleRaftReadyAppend(ctx *tempRaftContext, rd *raft.Ready) {
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

func (pr *PeerReplicate) handleRaftReadyApply(ctx *tempRaftContext, rd *raft.Ready) {
	if ctx.snapCell != nil {
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

func (pr *PeerReplicate) handleAppendSnapshot(ctx *tempRaftContext, rd *raft.Ready) {
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

func (pr *PeerReplicate) handleAppendEntries(ctx *tempRaftContext, rd *raft.Ready) {
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

func (pr *PeerReplicate) handleSaveRaftState(ctx *tempRaftContext) {
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

func (pr *PeerReplicate) handleSaveApplyState(ctx *tempRaftContext) {
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

func (pr *PeerReplicate) notifyPropose(meta *proposalMeta) {
	err := pr.proposes.Put(meta)
	if err != nil {
		log.Infof("raftstore[cell-%d]: raft propose stopped",
			pr.ps.getCell().ID)
		return
	}

	if log.DebugEnabled() {
		for _, req := range meta.cmd.req.Requests {
			log.Debugf("req: added to proposal queue. uuid=<%d>",
				req.UUID)
		}
	}

	queueGauge.WithLabelValues(labelQueuePropose).Set(float64(pr.proposes.Len()))

	pr.addEvent()
}

func (pr *PeerReplicate) preProposal(cmd *cmd) bool {
	// we handle all read, write and admin cmd here
	if cmd.req.Header == nil || cmd.req.Header.UUID == nil {
		cmd.resp(errorOtherCMDResp(errMissingUUIDCMD))
		return false
	}

	err := pr.store.validateStoreID(cmd.req)
	if err != nil {
		cmd.respOtherError(err)
		return false
	}

	// pr := s.replicatesMap.get(cmd.req.Header.CellId)
	// if nil == pr {
	// 	cmd.respCellNotFound(cmd.req.Header.CellId, 0)
	// 	return
	// }

	term := pr.getCurrentTerm()

	pe := pr.store.validateCell(cmd.req)
	if err != nil {
		cmd.resp(errorPbResp(pe, cmd.req.Header.UUID, term))
		return false
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
	meta := &proposalMeta{
		cmd:  cmd,
		term: term,
	}

	pr.notifyPropose(meta)

	return true
}

func (pr *PeerReplicate) propose(meta *proposalMeta) bool {
	if globalCfg.EnableRequestMetrics {
		observeRequestWaitting(meta.cmd)
	}

	if log.DebugEnabled() {
		for _, req := range meta.cmd.req.Requests {
			log.Debugf("req: start to proposal. uuid=<%d>",
				req.UUID)
		}
	}

	isConfChange := false
	policy, err := pr.getHandlePolicy(meta.cmd.req)
	if err != nil {
		resp := errorOtherCMDResp(err)
		meta.cmd.resp(resp)
		return true
	}

	doPropose := false
	switch policy {
	case readLocal:
		pr.execReadLocal(meta.cmd)
		return true
	case readIndex:
		return pr.execReadIndex(meta)
	case proposeNormal:
		doPropose = pr.proposeNormal(meta)
	case proposeTransferLeader:
		doPropose = pr.proposeTransferLeader(meta)
	case proposeChange:
		isConfChange = true
		doPropose = pr.proposeConfChange(meta)
	}

	if !doPropose {
		return true
	}

	err = pr.startProposeJob(meta, isConfChange)
	if err != nil {
		meta.cmd.respOtherError(err)
		return true
	}

	return false
}

func (pr *PeerReplicate) proposeNormal(meta *proposalMeta) bool {
	cmd := meta.cmd
	if !pr.isLeader() {
		cmd.respNotLeader(pr.cellID, meta.term, nil)
		return false
	}

	data := util.MustMarshal(cmd.req)
	size := uint64(len(data))

	raftFlowProposalSizeHistogram.Observe(float64(size))

	if size > globalCfg.Raft.MaxSizePerEntry {
		cmd.respLargeRaftEntrySize(pr.cellID, size, meta.term)
		return false
	}

	idx := pr.nextProposalIndex()
	err := pr.rn.Propose(data)
	if err != nil {
		cmd.resp(errorOtherCMDResp(err))
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		cmd.respNotLeader(pr.cellID, meta.term, nil)
		return false
	}

	if globalCfg.EnableRequestMetrics {
		observeRequestProposal(meta.cmd)
	}

	pr.metrics.propose.normal++
	return true
}

func (pr *PeerReplicate) proposeConfChange(meta *proposalMeta) bool {
	cmd := meta.cmd

	err := pr.checkConfChange(cmd)
	if err != nil {
		cmd.respOtherError(err)
		return false
	}

	changePeer := new(raftcmdpb.ChangePeerRequest)
	util.MustUnmarshal(changePeer, cmd.req.AdminRequest.Body)

	cc := new(raftpb.ConfChange)
	switch changePeer.ChangeType {
	case pdpb.AddNode:
		cc.Type = raftpb.ConfChangeAddNode
	case pdpb.RemoveNode:
		cc.Type = raftpb.ConfChangeRemoveNode
	}
	cc.NodeID = changePeer.Peer.ID
	cc.Context = util.MustMarshal(cmd.req)

	idx := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(*cc)
	if err != nil {
		cmd.respOtherError(err)
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		cmd.respNotLeader(pr.cellID, meta.term, nil)
		return false
	}

	log.Infof("raftstore[cell-%d]: propose conf change, type=<%s> peer=<%d>",
		pr.cellID,
		changePeer.ChangeType.String(),
		changePeer.Peer.ID)

	pr.metrics.propose.confChange++
	return true
}

func (pr *PeerReplicate) proposeTransferLeader(meta *proposalMeta) bool {
	cmd := meta.cmd
	req := new(raftcmdpb.TransferLeaderRequest)
	util.MustUnmarshal(req, cmd.req.AdminRequest.Body)

	if pr.isTransferLeaderAllowed(&req.Peer) {
		pr.doTransferLeader(&req.Peer)
	} else {
		log.Infof("raftstore[cell-%d]: transfer leader ignored directly, req=<%+v>",
			pr.cellID,
			req)
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	cmd.resp(newAdminRaftCMDResponse(raftcmdpb.TransferLeader, &raftcmdpb.TransferLeaderResponse{}))
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
func (pr *PeerReplicate) checkConfChange(cmd *cmd) error {
	data := cmd.req.AdminRequest.Body
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
	sendMsg := mraft.RaftMessage{}
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
	pr.store.trans.send(&sendMsg)

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
		pr.metrics.message.snapshot++
		pr.rn.ReportSnapshot(sendMsg.ToPeer.ID, raft.SnapshotFinish)
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

func (pr *PeerReplicate) reportUnreachable(msg raftpb.Message) {
	err := pr.reports.Put(msg)
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
		ElectionTick:    globalCfg.Raft.ElectionTick,
		HeartbeatTick:   globalCfg.Raft.HeartbeatTick,
		MaxSizePerMsg:   globalCfg.Raft.MaxSizePerMsg,
		MaxInflightMsgs: globalCfg.Raft.MaxInflightMsgs,
		Storage:         store,
		CheckQuorum:     true,
		PreVote:         false,
	}
}
