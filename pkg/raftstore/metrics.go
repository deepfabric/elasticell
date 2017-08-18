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

func init() {
	initMetricsForRaft()
	initMetricsForCommand()
	initMetricsForStore()
	initMetricsForSnapshot()
	initMetricsForRequest()
}

type localMetrics struct {
	ready   raftReadyMetrics
	message raftMessageMetrics
	propose raftProposeMetrics
	admin   raftAdminMetrics
}

func (m *localMetrics) flush() {
	m.ready.flush()
	m.message.flush()
	m.propose.flush()
	m.admin.flush()
}

type raftReadyMetrics struct {
	message   uint64
	commit    uint64
	append    uint64
	snapshort uint64
}

func (m *raftReadyMetrics) flush() {
	if m.message > 0 {
		raftFlowReadyCounterVec.WithLabelValues(labelRaftFlowSentMsg).Add(float64(m.message))
		m.message = 0
	}

	if m.commit > 0 {
		raftFlowReadyCounterVec.WithLabelValues(labelRaftFlowCommit).Add(float64(m.commit))
		m.commit = 0
	}

	if m.append > 0 {
		raftFlowReadyCounterVec.WithLabelValues(labelRaftFlowAppend).Add(float64(m.append))
		m.append = 0
	}

	if m.snapshort > 0 {
		raftFlowReadyCounterVec.WithLabelValues(labelRaftFlowSnapshot).Add(float64(m.snapshort))
		m.snapshort = 0
	}
}

type raftMessageMetrics struct {
	append        uint64
	appendResp    uint64
	vote          uint64
	voteResp      uint64
	snapshot      uint64
	heartbeat     uint64
	heartbeatResp uint64
	transfeLeader uint64
}

func (m *raftMessageMetrics) flush() {
	if m.append > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentAppend).Add(float64(m.append))
		m.append = 0
	}

	if m.appendResp > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentAppendResp).Add(float64(m.appendResp))
		m.appendResp = 0
	}

	if m.vote > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentVote).Add(float64(m.vote))
		m.vote = 0
	}

	if m.voteResp > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentVoteResp).Add(float64(m.voteResp))
		m.voteResp = 0
	}

	if m.snapshot > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentSnapshot).Add(float64(m.snapshot))
		m.snapshot = 0
	}

	if m.heartbeat > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentHeartbeat).Add(float64(m.heartbeat))
		m.heartbeat = 0
	}

	if m.heartbeatResp > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentHeartbeatResp).Add(float64(m.heartbeatResp))
		m.heartbeatResp = 0
	}

	if m.transfeLeader > 0 {
		raftFlowSentMsgCounterVec.WithLabelValues(labelRaftFlowSentTransfeLeader).Add(float64(m.transfeLeader))
		m.transfeLeader = 0
	}
}

type raftProposeMetrics struct {
	readLocal      uint64
	readIndex      uint64
	normal         uint64
	transferLeader uint64
	confChange     uint64
}

func (m *raftProposeMetrics) flush() {
	if m.readLocal > 0 {
		raftFlowProposalCounterVec.WithLabelValues(labelRaftFlowProposalReadLocal).Add(float64(m.readLocal))
		m.readLocal = 0
	}

	if m.readIndex > 0 {
		raftFlowProposalCounterVec.WithLabelValues(labelRaftFlowProposalReadIndex).Add(float64(m.readIndex))
		m.readIndex = 0
	}

	if m.normal > 0 {
		raftFlowProposalCounterVec.WithLabelValues(labelRaftFlowProposalNormal).Add(float64(m.normal))
		m.normal = 0
	}

	if m.transferLeader > 0 {
		raftFlowProposalCounterVec.WithLabelValues(labelRaftFlowProposalTransferLeader).Add(float64(m.transferLeader))
		m.transferLeader = 0
	}

	if m.confChange > 0 {
		raftFlowProposalCounterVec.WithLabelValues(labelRaftFlowProposalConfChange).Add(float64(m.confChange))
		m.confChange = 0
	}
}

type raftAdminMetrics struct {
	confChange uint64
	addPeer    uint64
	removePeer uint64
	split      uint64
	compact    uint64

	confChangeReject uint64

	confChangeSucceed uint64
	addPeerSucceed    uint64
	removePeerSucceed uint64
	splitSucceed      uint64
	compactSucceed    uint64
}

func (m *raftAdminMetrics) incBy(by raftAdminMetrics) {
	m.confChange += by.confChange
	m.confChangeSucceed += by.confChangeSucceed
	m.confChangeReject += by.confChangeReject
	m.addPeer += by.addPeer
	m.addPeerSucceed += by.addPeerSucceed
	m.removePeer += by.removePeer
	m.removePeerSucceed += by.removePeerSucceed
	m.split += by.split
	m.splitSucceed += by.splitSucceed
	m.compact += by.compact
	m.compactSucceed += by.compactSucceed
}

func (m *raftAdminMetrics) flush() {
	if m.confChange > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminConfChange, labelCommandAdminPerAll).Add(float64(m.confChange))
		m.confChange = 0
	}

	if m.addPeer > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminAddPeer, labelCommandAdminPerAll).Add(float64(m.addPeer))
		m.addPeer = 0
	}

	if m.removePeer > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminRemovePeer, labelCommandAdminPerAll).Add(float64(m.removePeer))
		m.removePeer = 0
	}

	if m.split > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminSplit, labelCommandAdminPerAll).Add(float64(m.split))
		m.split = 0
	}

	if m.compact > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminCompact, labelCommandAdminPerAll).Add(float64(m.compact))
		m.compact = 0
	}

	if m.confChangeSucceed > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminConfChange, labelCommandAdminSucceed).Add(float64(m.confChangeSucceed))
		m.confChangeSucceed = 0
	}

	if m.addPeerSucceed > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminAddPeer, labelCommandAdminSucceed).Add(float64(m.addPeerSucceed))
		m.addPeerSucceed = 0
	}

	if m.removePeerSucceed > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminRemovePeer, labelCommandAdminSucceed).Add(float64(m.removePeerSucceed))
		m.removePeerSucceed = 0
	}

	if m.splitSucceed > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminSplit, labelCommandAdminSucceed).Add(float64(m.splitSucceed))
		m.splitSucceed = 0
	}

	if m.compactSucceed > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminCompact, labelCommandAdminSucceed).Add(float64(m.compactSucceed))
		m.compactSucceed = 0
	}

	if m.confChangeReject > 0 {
		commandAdminCounterVec.WithLabelValues(labelCommandAdminConfChange, labelCommandAdminRejectUnsafe).Add(float64(m.confChangeReject))
		m.confChangeReject = 0
	}
}
