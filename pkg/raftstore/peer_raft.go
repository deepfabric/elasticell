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

func (p *PeerReplicate) serveRaft() error {
	for {
		select {
		case <-p.raftTicker.C:
			p.rn.Tick()

		case rd := <-p.rn.Ready():
			ctx := &tempRaftContext{
				raftState:  mraft.RaftLocalState{},
				applyState: mraft.RaftApplyState{},
				lastTerm:   0,
			}

			p.handleRaftReadyAppend(ctx, rd)
			p.postHandleRaftReadyAppend(ctx, rd)
			// r.maybeTriggerSnapshot()
			// r.rn.Advance()
		}
	}
}

func (p *PeerReplicate) handleRaftReadyAppend(ctx *tempRaftContext, rd raft.Ready) {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if p.ps.isApplyingSnap() {
		log.Debugf("raftstore[cell-%d]: still applying snapshot, skip further handling", p.ps.cell.ID)
		return
	}

	p.ps.resetApplyingSnapJob()

	if !raft.IsEmptySnap(rd.Snapshot) &&
		p.ps.raftState.HardState.Commit != p.ps.applyState.AppliedIndex {
		log.Debugf("raftstore[cell-%d]: apply index and commit index not match, skip applying snapshot, apply=<%d> commit=<%d>",
			p.ps.cell.ID,
			p.ps.applyState.AppliedIndex,
			p.ps.raftState.HardState.Commit)
		return
	}

	// TODO: on_role_changed

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if p.isLeader() {
		p.send(rd.Messages)
	}

	p.handleApplySnapshot(ctx, rd)
	p.handleAppendEntries(ctx, rd)

	if ctx.raftState.LastIndex > 0 &&
		(rd.HardState.Commit != 0 ||
			rd.HardState.Term != 0 ||
			rd.HardState.Vote != 0) {
		ctx.raftState.HardState = rd.HardState
	}

	p.handleSaveRaftState(ctx)
	p.handleSaveApplyState(ctx)

	// TODO: batch write to rocksdb
}

func (p *PeerReplicate) postHandleRaftReadyAppend(ctx *tempRaftContext, rd raft.Ready) *applySnapResult {
	// TODO: impl
	// if invoke_ctx.has_snapshot() {
	//         // When apply snapshot, there is no log applied and not compacted yet.
	//         self.raft_log_size_hint = 0;
	//     }

	result := p.ps.handleDoPostReady(ctx)

	if !p.isLeader() {
		p.send(rd.Messages)
	}

	if result != nil {
		// TODO: impl let reg = ApplyTask::register(self);
		// self.apply_scheduler.schedule(reg).unwrap();
	}

	return result
}

func (p *PeerReplicate) handleApplySnapshot(ctx *tempRaftContext, rd raft.Ready) {
	if !raft.IsEmptySnap(rd.Snapshot) {
		err := p.getStore().handleDoApplySnapshot(ctx, rd.Snapshot)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				p.ps.cell.ID,
				err)
		}
	}
}

func (p *PeerReplicate) handleAppendEntries(ctx *tempRaftContext, rd raft.Ready) {
	if len(rd.Entries) > 0 {
		err := p.getStore().handleDoAppendEntries(ctx, rd.Entries)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				p.ps.cell.ID,
				err)
		}
	}
}

func (p *PeerReplicate) handleSaveRaftState(ctx *tempRaftContext) {
	tmp := ctx.raftState
	origin := p.ps.raftState

	if tmp.LastIndex != origin.LastIndex ||
		tmp.HardState.Commit != origin.HardState.Commit ||
		tmp.HardState.Term != origin.HardState.Term ||
		tmp.HardState.Vote != origin.HardState.Vote {
		err := p.ps.handleDoSaveRaftState(ctx)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				p.ps.cell.ID,
				err)
		}
	}
}

func (p *PeerReplicate) handleSaveApplyState(ctx *tempRaftContext) {
	tmp := ctx.applyState
	origin := p.ps.applyState

	if tmp.AppliedIndex != origin.AppliedIndex ||
		tmp.TruncatedState.Index != origin.TruncatedState.Index ||
		tmp.TruncatedState.Term != origin.TruncatedState.Term {
		err := p.ps.handleDoSaveApplyState(ctx)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: handle raft ready failure, errors:\n %+v",
				p.ps.cell.ID,
				err)
		}
	}
}

func (p *PeerReplicate) isLeader() bool {
	return p.rn.Status().RaftState == raft.StateLeader
}

func (p *PeerReplicate) send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		p.msgC <- msg
	}
}

func (p *PeerReplicate) doSendFromChan(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("raftstore[cell-%d]: server stopped",
				p.ps.cell.ID)
			close(p.msgC)
			return
		// TODO: use queue instead of chan
		case msg := <-p.msgC:
			err := p.sendRaftMsg(msg)
			if err != nil {
				// We don't care that the message is sent failed, so here just log this error
				log.Warnf("raftstore[cell-%d]: send msg failure, error:\n %+v",
					p.ps.cell.ID,
					err)
			}
		}
	}
}

func (p *PeerReplicate) sendRaftMsg(msg raftpb.Message) error {
	sendMsg := mraft.RaftMessage{}
	sendMsg.CellID = p.ps.cell.ID
	sendMsg.CellEpoch = p.ps.cell.Epoch

	sendMsg.FromPeer = p.peer
	sendMsg.ToPeer = p.store.peerCache.get(msg.To)
	if sendMsg.ToPeer.ID == 0 {
		return fmt.Errorf("can not found peer<%d>", msg.To)
	}

	if log.DebugEnabled() {
		log.Debugf("raftstore[cell-%d]: send raft msg, from=<%d> to=<%d> msg=<%s>",
			p.ps.cell.ID,
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
	if p.ps.isInitialized() &&
		(msg.Type == raftpb.MsgVote ||
			// the peer has not been known to this leader, it may exist or not.
			(msg.Type == raftpb.MsgHeartbeat && msg.Commit == 0)) {
		sendMsg.Start = p.ps.cell.Start
		sendMsg.End = p.ps.cell.End
	}

	sendMsg.Message = msg
	// TODO: get to peer addr
	err := p.store.trans.send("", &sendMsg)
	if err != nil {
		log.Warnf("raftstore[cell-%d]: failed to send msg, from=<%d> to=<%d>",
			sendMsg.FromPeer.ID,
			sendMsg.ToPeer.ID)

		p.rn.ReportUnreachable(sendMsg.ToPeer.ID)

		if msg.Type == raftpb.MsgSnap {
			p.rn.ReportSnapshot(sendMsg.ToPeer.ID, raft.SnapshotFailure)
		}
	}

	return nil
}

func (p *PeerReplicate) step(msg raftpb.Message) error {
	return p.rn.Step(context.TODO(), msg)
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
