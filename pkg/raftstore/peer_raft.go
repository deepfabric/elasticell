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

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
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
	uuid []byte
	term uint64
}

// TODO: use a better impl.
type proposalQueue struct {
	queue []*proposalMeta
	uuids map[string]struct{}
}

func newProposalQueue() *proposalQueue {
	return &proposalQueue{
		queue: make([]*proposalMeta, 0, 1024),
		uuids: make(map[string]struct{}, 1024),
	}
}

func (q *proposalQueue) contains(uuid []byte) bool {
	key := util.SliceToString(uuid)
	_, ok := q.uuids[key]
	return ok
}

func (q *proposalQueue) pop(term uint64) *proposalMeta {
	if len(q.queue) <= 0 {
		return nil
	}

	m := q.queue[0]

	if m.term > term {
		return nil
	}

	q.queue[0] = nil
	q.queue = q.queue[1:]
	delete(q.uuids, util.SliceToString(m.uuid))
	return m
}

func (q *proposalQueue) push(meta *proposalMeta) {
	q.uuids[util.SliceToString(meta.uuid)] = emptyStruct
	q.queue = append(q.queue, meta)
}

func (q *proposalQueue) clear() {
	for k := range q.uuids {
		delete(q.uuids, k)
	}

	q.queue = make([]*proposalMeta, 0, 1024)
}

func (pr *PeerReplicate) serveRaft() error {
	for {
		select {
		case <-pr.raftTicker.C:
			pr.rn.Tick()

		case rd := <-pr.rn.Ready():
			ctx := &tempRaftContext{
				raftState:  mraft.RaftLocalState{},
				applyState: mraft.RaftApplyState{},
				lastTerm:   0,
			}

			pr.handleRaftReadyAppend(ctx, &rd)
			pr.handleRaftReadyApply(ctx, &rd)
		}
	}
}

func (pr *PeerReplicate) handleRaftReadyAppend(ctx *tempRaftContext, rd *raft.Ready) {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if pr.ps.isApplyingSnap() {
		log.Debugf("raftstore[cell-%d]: still applying snapshot, skip further handling", pr.ps.getCell().ID)
		return
	}

	pr.ps.resetApplyingSnapJob()

	// wait apply committed entries complete
	if !raft.IsEmptySnap(rd.Snapshot) &&
		!pr.ps.isApplyComplete() {
		log.Debugf("raftstore[cell-%d]: apply index and committed index not match, skip applying snapshot, apply=<%d> commit=<%d>",
			pr.ps.getCell().ID,
			pr.ps.getAppliedIndex(),
			pr.ps.raftState.HardState.Commit)
		return
	}

	// If we become leader, send heartbeat to pd
	if rd.SoftState != nil {
		if rd.SoftState.RaftState == raft.StateLeader {
			pr.handleHeartbeat()
		}
	}

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if pr.isLeader() {
		pr.send(rd.Messages)
	}

	pr.handleAppendSnapshot(ctx, rd)
	pr.handleAppendEntries(ctx, rd)

	if ctx.raftState.LastIndex > 0 && !raft.IsEmptyHardState(rd.HardState) {
		ctx.raftState.HardState = rd.HardState
	}

	pr.handleSaveRaftState(ctx)
	pr.handleSaveApplyState(ctx)

	// TODO: use write batch
}

func (pr *PeerReplicate) handleRaftReadyApply(ctx *tempRaftContext, rd *raft.Ready) {
	result := pr.doApplySnap(ctx)

	if !pr.isLeader() {
		pr.send(rd.Messages)
	}

	if result != nil {
		pr.startRegistrationJob()
	}

	asyncApplyCommitted := pr.applyCommittedEntries(rd)

	pr.doApplyReads(rd)

	if result != nil {
		pr.updateKeyRange(result)
	}

	// if has none async job, so we can direct advance raft,
	// otherwise we need advance raft when our async job has finished
	if !asyncApplyCommitted && result == nil {
		pr.rn.Advance()
	}
}

func (pr *PeerReplicate) handleAppendSnapshot(ctx *tempRaftContext, rd *raft.Ready) {
	if !raft.IsEmptySnap(rd.Snapshot) {
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

func (pr *PeerReplicate) readyToProcessPropose(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("raftstore[cell-%d]: cell propose exit.", pr.cellID)
			close(pr.proposeC)
			return
		case meta := <-pr.proposeC:
			pr.propose(meta)
		}
	}
}

func (pr *PeerReplicate) notifyPropose(meta *proposalMeta) {
	pr.proposeC <- meta
}

func (pr *PeerReplicate) propose(meta *proposalMeta) {
	if pr.proposals.contains(meta.uuid) {
		resp := errorOtherCMDResp(fmt.Errorf("duplicated uuid %v", meta.uuid))
		meta.cmd.resp(resp)
		return
	}

	log.Debugf("raftstore[cell-%d]: propose command, uuid=<%v>", meta.uuid)

	isConfChange := false
	policy, err := pr.getHandlePolicy(meta.cmd.req)
	if err != nil {
		resp := errorOtherCMDResp(err)
		meta.cmd.resp(resp)
		return
	}

	switch policy {
	case readLocal:
		pr.execReadLocal(meta.cmd)
		return
	case readIndex:
		pr.execReadIndex(meta.cmd)
		return
	case proposeNormal:
		pr.proposeNormal(meta)
	case proposeTransferLeader:
		pr.proposeTransferLeader(meta)
	case proposeChange:
		isConfChange = true
		pr.proposeConfChange(meta)
	}

	err = pr.startProposeJob(meta, isConfChange)
	if err != nil {
		resp := errorOtherCMDResp(err)
		meta.cmd.resp(resp)
		return
	}
	pr.proposals.push(meta)
}

func (pr *PeerReplicate) proposeNormal(meta *proposalMeta) {
	cmd := meta.cmd
	data := util.MustMarshal(cmd.req)

	size := uint64(len(data))
	if size > pr.store.cfg.Raft.MaxSizePerEntry {
		cmd.respLargeRaftEntrySize(pr.cellID, size, meta.term)
		return
	}

	idx := pr.nextProposalIndex()
	err := pr.rn.Propose(context.TODO(), data)
	if err != nil {
		cmd.resp(errorOtherCMDResp(err))
		return
	}
	if idx == pr.nextProposalIndex() {
		cmd.respNotLeader(pr.cellID, meta.term, nil)
		return
	}
}

func (pr *PeerReplicate) proposeConfChange(meta *proposalMeta) {
	cmd := meta.cmd

	err := pr.checkConfChange(cmd)
	if err != nil {
		cmd.respOtherError(err)
		return
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

	log.Infof("raftstore[cell-%d]: propose conf change, type=<%s> peer=<%d>",
		pr.cellID,
		changePeer.ChangeType.String(),
		changePeer.Peer.ID)

	idx := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(context.TODO(), *cc)
	if err != nil {
		cmd.respOtherError(err)
		return
	}
	if idx == pr.nextProposalIndex() {
		cmd.respNotLeader(pr.cellID, meta.term, nil)
		return
	}
}

func (pr *PeerReplicate) proposeTransferLeader(meta *proposalMeta) {
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
}

func (pr *PeerReplicate) doTransferLeader(peer *metapb.Peer) {
	log.Infof("raftstore[cell-%d]: transfer leader to %d",
		pr.cellID,
		peer.ID)
	pr.rn.TransferLeadership(context.TODO(), pr.rn.Status().Lead, peer.ID)
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
	idx, _ := pr.ps.LastIndex()
	return idx + 1
}

func (pr *PeerReplicate) isLeader() bool {
	return pr.rn.Status().RaftState == raft.StateLeader
}

func (pr *PeerReplicate) send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		pr.msgC <- msg
	}
}

func (pr *PeerReplicate) readyToSendRaftMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("raftstore[cell-%d]: peer send msg raft stopped",
				pr.ps.getCell().ID)
			close(pr.msgC)
			return
		case msg := <-pr.msgC:
			err := pr.sendRaftMsg(msg)
			if err != nil {
				// We don't care that the message is sent failed, so here just log this error
				log.Warnf("raftstore[cell-%d]: send msg failure, error:\n %+v",
					pr.ps.getCell().ID,
					err)
			}
		}
	}
}

func (pr *PeerReplicate) sendRaftMsg(msg raftpb.Message) error {
	sendMsg := mraft.RaftMessage{}
	sendMsg.CellID = pr.ps.getCell().ID
	sendMsg.CellEpoch = pr.ps.getCell().Epoch

	sendMsg.FromPeer = pr.peer
	sendMsg.ToPeer, _ = pr.store.peerCache.get(msg.To)
	if sendMsg.ToPeer.ID == 0 {
		return fmt.Errorf("can not found peer<%d>", msg.To)
	}

	if log.DebugEnabled() {
		log.Debugf("raftstore[cell-%d]: send raft msg, from=<%d> to=<%d> msg=<%s>",
			pr.ps.getCell().ID,
			sendMsg.FromPeer.ID,
			sendMsg.ToPeer.ID,
			msg.String())
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
	err := pr.store.trans.send(sendMsg.ToPeer.StoreID, &sendMsg)
	if err != nil {
		log.Warnf("raftstore[cell-%d]: failed to send msg, from=<%d> to=<%d>",
			sendMsg.FromPeer.ID,
			sendMsg.ToPeer.ID)

		pr.rn.ReportUnreachable(sendMsg.ToPeer.ID)

		if msg.Type == raftpb.MsgSnap {
			pr.rn.ReportSnapshot(sendMsg.ToPeer.ID, raft.SnapshotFailure)
		}
	}

	return nil
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
		// TODO: match redis command
		switch r.Type {
		case raftcmdpb.Get:
			isRead = true
		}
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
	if pr.isLeader() && msg.From != 0 {
		pr.peerHeartbeatsMap.put(msg.From, time.Now())
	}
	return pr.rn.Step(context.TODO(), msg)
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
