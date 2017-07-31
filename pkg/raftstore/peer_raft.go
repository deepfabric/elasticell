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
)

type proposalMeta struct {
	cmd  *cmd
	term uint64
}

func (pr *PeerReplicate) onRaftTick(arg interface{}) {
	if pr.isStopped() {
		log.Infof("raftstore[cell-%d]: raft tick stopped",
			pr.ps.getCell().ID)
		return
	}

	pr.tickC <- emptyStruct
	util.DefaultTimeoutWheel().Schedule(pr.store.cfg.getRaftBaseTickDuration(), pr.onRaftTick, nil)
}

func (pr *PeerReplicate) onRaftLoop(arg interface{}) {
	if pr.isStopped() {
		log.Infof("raftstore[cell-%d]: raft loop stopped",
			pr.ps.getCell().ID)
		return
	}

	pr.raftReady()
	util.DefaultTimeoutWheel().Schedule(time.Millisecond*50, pr.onRaftLoop, nil)
}

func (pr *PeerReplicate) raftReady() {
	pr.loopC <- emptyStruct
}

func (pr *PeerReplicate) readyToServeRaft(ctx context.Context) {
	pr.onRaftTick(nil)
	pr.onRaftLoop(nil)

	for {
		select {
		case <-ctx.Done():
			pr.setStop()

			close(pr.tickC)
			close(pr.loopC)
			close(pr.applyResultC)
			close(pr.stepC)
			close(pr.proposeC)
			close(pr.reqCtxC)

			log.Infof("raftstore[cell-%d]: handle serve raft stopped",
				pr.cellID)
			return
		case <-pr.tickC:
			pr.rn.Tick()
		case msg := <-pr.stepC:
			if nil != msg {
				if pr.isLeader() && msg.From != 0 {
					pr.peerHeartbeatsMap.put(msg.From, time.Now())
				}

				err := pr.rn.Step(*msg)
				if err != nil {
					pr.raftReady()
				}
			}
		case reqCtx := <-pr.reqCtxC:
			if nil != reqCtx {
				pr.batch.push(reqCtx)
				if pr.batch.isPreEntriesSaved() {
					pr.proposeNextBatch()
				}
			}
		case <-pr.loopC:
			pr.doRaftReady()
			pr.handlePollApply()
		case meta := <-pr.proposeC:
			if nil != meta {
				pr.propose(meta)
			}
		}
	}
}

func (pr *PeerReplicate) doRaftReady() {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if pr.ps.isApplyingSnap() {
		log.Debugf("raftstore[cell-%d]: still applying snapshot, skip further handling",
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
		log.Debugf("raftstore[cell-%d]: raft ready, last ready index=<%d>",
			pr.cellID,
			pr.ps.lastReadyIndex)
		ctx := &tempRaftContext{
			raftState:  pr.ps.raftState,
			applyState: pr.ps.applyState,
			lastTerm:   pr.ps.lastTerm,
		}

		pr.handleRaftReadyAppend(ctx, &rd)
		pr.handleRaftReadyApply(ctx, &rd)
	}
}

func (pr *PeerReplicate) addApplyResult(result *asyncApplyResult) {
	if pr.isStopped() {
		return
	}

	pr.applyResultC <- result
}

func (pr *PeerReplicate) handlePollApply() {
	for {
		select {
		case result := <-pr.applyResultC:
			pr.doPollApply(result)
		default:
			return
		}
	}
}

func (pr *PeerReplicate) doPollApply(result *asyncApplyResult) {
	pr.doPostApply(result)
	if result.result != nil {
		pr.store.doPostApplyResult(result)
	}

	pr.proposeNextBatch()
}

func (pr *PeerReplicate) handleRaftReadyAppend(ctx *tempRaftContext, rd *raft.Ready) {
	// If we become leader, send heartbeat to pd
	if rd.SoftState != nil {
		if rd.SoftState.RaftState == raft.StateLeader {
			log.Infof("raftstore[cell-%d]: ********become leader now********",
				pr.cellID)
			pr.doHeartbeat()
			pr.proposeNextBatch()
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

func (pr *PeerReplicate) readyToReceiveCMD(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(pr.batchC)
			log.Infof("raftstore[cell-%d]: handle req stopped", pr.cellID)
			return
		case c := <-pr.batchC:
			if nil != c {
				pr.store.notify(c)
			}
		}
	}
}

func (pr *PeerReplicate) notifyPropose(meta *proposalMeta) {
	if pr.isStopped() {
		return
	}

	pr.proposeC <- meta
}

func (pr *PeerReplicate) propose(meta *proposalMeta) {
	log.Debugf("raftstore[cell-%d]: handle propose, meta=<%+v>",
		pr.cellID,
		meta)

	isConfChange := false
	policy, err := pr.getHandlePolicy(meta.cmd.req)
	if err != nil {
		resp := errorOtherCMDResp(err)
		meta.cmd.resp(resp)
		return
	}

	doPropose := false
	switch policy {
	case readLocal:
		pr.execReadLocal(meta.cmd)
		return
	case readIndex:
		pr.execReadIndex(meta)
		return
	case proposeNormal:
		doPropose = pr.proposeNormal(meta)
	case proposeTransferLeader:
		doPropose = pr.proposeTransferLeader(meta)
	case proposeChange:
		isConfChange = true
		doPropose = pr.proposeConfChange(meta)
	}

	if !doPropose {
		return
	}

	err = pr.startProposeJob(meta, isConfChange)
	if err != nil {
		meta.cmd.respOtherError(err)
		return
	}
}

func (pr *PeerReplicate) proposeNormal(meta *proposalMeta) bool {
	cmd := meta.cmd
	if !pr.isLeader() {
		cmd.respNotLeader(pr.cellID, meta.term, nil)
		return false
	}

	data := util.MustMarshal(cmd.req)
	size := uint64(len(data))
	if size > pr.store.cfg.Raft.MaxSizePerEntry {
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
	pr.raftReady()
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

	pr.raftReady()
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
	pr.raftReady()
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
	if pr.isStopped() {
		return
	}

	for _, msg := range msgs {
		err := pr.sendRaftMsg(msg)
		if err != nil {
			// We don't care that the message is sent failed, so here just log this error
			log.Warnf("raftstore[cell-%d]: send msg failure, from_peer=<%d> to_peer=<%d>",
				pr.ps.getCell().ID,
				msg.From,
				msg.To)
		}
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

	if msg.Type == raftpb.MsgSnap {
		pr.rn.ReportSnapshot(sendMsg.ToPeer.ID, raft.SnapshotFinish)
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

func (pr *PeerReplicate) step(msg raftpb.Message) error {
	pr.stepC <- &msg
	return nil
}

func getRaftConfig(id, appliedIndex uint64, store raft.Storage, cfg *RaftCfg) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		Storage:         store,
		CheckQuorum:     true,
		PreVote:         false,
	}
}
