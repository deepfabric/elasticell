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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	labelRaftFlowSentMsg  = "message"
	labelRaftFlowCommit   = "commit"
	labelRaftFlowAppend   = "append"
	labelRaftFlowSnapshot = "snapshot"

	labelRaftFlowProposalReadLocal      = "read_local"
	labelRaftFlowProposalReadIndex      = "read_index"
	labelRaftFlowProposalNormal         = "normal"
	labelRaftFlowProposalConfChange     = "conf_change"
	labelRaftFlowProposalTransferLeader = "transfer_leader"

	labelRaftFlowSentAppend        = "append"
	labelRaftFlowSentAppendResp    = "append_resp"
	labelRaftFlowSentVote          = "vote"
	labelRaftFlowSentVoteResp      = "vote_resp"
	labelRaftFlowSentSnapshot      = "snapshot"
	labelRaftFlowSentHeartbeat     = "heartbeat"
	labelRaftFlowSentHeartbeatResp = "heartbeat_resp"
	labelRaftFlowSentTransfeLeader = "transfe_leader"

	labelRaftFlowFailureReportUnreachable = "unreachable"
	labelRaftFlowFailureReportSnapshot    = "snapshot"
)

// metrics for raft log
var (
	raftLogAppendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_log_append_duration_seconds",
			Help:      "Bucketed histogram of peer appending log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	raftLogApplyDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_log_apply_duration_seconds",
			Help:      "Bucketed histogram of peer appending log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	raftLogLagHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_log_lag",
			Help:      "Bucketed histogram of log lag in a cell.",
			Buckets:   []float64{2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 5120.0, 10240.0},
		})

	raftLogCompactCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_log_compact_total",
			Help:      "Total number of compact raft log.",
		})
)

// metrics for raft flow
var (
	raftFlowProposalSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_flow_proposal_log_bytes",
			Help:      "Bucketed histogram of peer proposing log size.",
			Buckets:   []float64{256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0, 2097152.0, 4194304.0, 8388608.0, 16777216.0},
		})

	raftFlowProposalCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_flow_proposal_total",
			Help:      "Total number of proposal made.",
		}, []string{"type"})

	raftFlowReadyCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_flow_ready_handled_total",
			Help:      "Total number of raft ready handled.",
		}, []string{"type"})

	raftFlowSentMsgCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_flow_sent_msg_total",
			Help:      "Total number of raft ready sent messages.",
		}, []string{"type"})

	raftFlowProcessReadyDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_flow_process_ready_duration_seconds",
			Help:      "Bucketed histogram of peer processing raft ready duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	raftFlowFailureReportCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "raft_flow_failure_report_total",
			Help:      "Total number of reporting failure messages.",
		}, []string{"type", "store_id"})
)

func initMetricsForRaft() {
	prometheus.MustRegister(raftLogCompactCounter)
	prometheus.MustRegister(raftLogLagHistogram)
	prometheus.MustRegister(raftFlowProposalCounterVec)
	prometheus.MustRegister(raftLogAppendDurationHistogram)
	prometheus.MustRegister(raftLogApplyDurationHistogram)
	prometheus.MustRegister(raftFlowReadyCounterVec)
	prometheus.MustRegister(raftFlowProcessReadyDurationHistogram)
	prometheus.MustRegister(raftFlowProposalSizeHistogram)
	prometheus.MustRegister(raftFlowFailureReportCounterVec)
	prometheus.MustRegister(raftFlowSentMsgCounterVec)
}

func observeRaftLogAppend(start time.Time) {
	raftLogAppendDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

func observeRaftLogApply(start time.Time) {
	raftLogApplyDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

func observeRaftFlowProcessReady(start time.Time) {
	raftFlowProcessReadyDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}
