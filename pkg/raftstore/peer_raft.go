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
	"fmt"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"golang.org/x/net/context"
)

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
			// TODO: think about advice raft
		}
	}
}

func (pr *PeerReplicate) handleRaftReadyAppend(ctx *tempRaftContext, rd *raft.Ready) {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if pr.ps.isApplyingSnap() {
		log.Debugf("raftstore[cell-%d]: still applying snapshot, skip further handling", pr.ps.cell.ID)
		return
	}

	pr.ps.resetApplyingSnapJob()

	// wait apply committed entries complete
	if !raft.IsEmptySnap(rd.Snapshot) &&
		!pr.ps.isApplyComplete() {
		log.Debugf("raftstore[cell-%d]: apply index and committed index not match, skip applying snapshot, apply=<%d> commit=<%d>",
			pr.ps.cell.ID,
			pr.ps.getAppliedIndex(),
			pr.ps.raftState.HardState.Commit)
		return
	}

	// TODO: on_role_changed

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

	// TODO: batch write to rocksdb
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
				pr.ps.cell.ID,
				err)
		}
	}
}

func (pr *PeerReplicate) handleAppendEntries(ctx *tempRaftContext, rd *raft.Ready) {
	if len(rd.Entries) > 0 {
		err := pr.getStore().doAppendEntries(ctx, rd.Entries)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				pr.ps.cell.ID,
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
				pr.ps.cell.ID,
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
				pr.ps.cell.ID,
				err)
		}
	}
}

func (pr *PeerReplicate) isLeader() bool {
	return pr.rn.Status().RaftState == raft.StateLeader
}

func (pr *PeerReplicate) send(msgs []raftpb.Message) {
	// TODO: impl use queue instead of chan
	for _, msg := range msgs {
		pr.msgC <- msg
	}
}

func (pr *PeerReplicate) doSendFromChan(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("raftstore[cell-%d]: server stopped",
				pr.ps.cell.ID)
			close(pr.msgC)
			return
		// TODO: use queue instead of chan
		case msg := <-pr.msgC:
			err := pr.sendRaftMsg(msg)
			if err != nil {
				// We don't care that the message is sent failed, so here just log this error
				log.Warnf("raftstore[cell-%d]: send msg failure, error:\n %+v",
					pr.ps.cell.ID,
					err)
			}
		}
	}
}

func (pr *PeerReplicate) sendRaftMsg(msg raftpb.Message) error {
	sendMsg := mraft.RaftMessage{}
	sendMsg.CellID = pr.ps.cell.ID
	sendMsg.CellEpoch = pr.ps.cell.Epoch

	sendMsg.FromPeer = pr.peer
	sendMsg.ToPeer = pr.store.peerCache.get(msg.To)
	if sendMsg.ToPeer.ID == 0 {
		return fmt.Errorf("can not found peer<%d>", msg.To)
	}

	if log.DebugEnabled() {
		log.Debugf("raftstore[cell-%d]: send raft msg, from=<%d> to=<%d> msg=<%s>",
			pr.ps.cell.ID,
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
		sendMsg.Start = pr.ps.cell.Start
		sendMsg.End = pr.ps.cell.End
	}

	sendMsg.Message = msg
	// TODO: get to peer addr
	err := pr.store.trans.send("", &sendMsg)
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

func (pr *PeerReplicate) getCurrentTerm() uint64 {
	return pr.rn.Status().Term
}

func (pr *PeerReplicate) step(msg raftpb.Message) error {
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
