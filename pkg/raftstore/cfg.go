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
	"time"
)

// Cfg for raftstore
type Cfg struct {
	StoreAddr                string `json:"storeAddr"`
	StoreAdvertiseAddr       string `json:"storeAdvertiseAddr"`
	StoreDataPath            string `json:"storeDataPath"`
	StoreHeartbeatIntervalMs int    `json:"storeHeartbeatIntervalMs"`
	CellHeartbeatIntervalMs  int    `json:"cellHeartbeatIntervalMs"`
	MaxPeerDownSec           int    `json:"maxPeerDownSec"`
	SplitCellCheckIntervalMs int    `json:"splitCellCheckIntervalMs"`
	RaftGCLogIntervalMs      int    `json:"raftGCLogIntervalMs"`
	ReportCellIntervalMs     int    `json:"reportCellIntervalMs"`

	RaftLogGCCountLimit   uint64 `json:"raftLogGCCountLimit"`
	RaftLogGCSizeLimit    uint64 `json:"raftLogGCSizeLimit"`
	RaftLogGCThreshold    uint64 `json:"raftLogGCThreshold"`
	RaftLogGCLagThreshold uint64 `json:"raftLogGCLagThreshold"`

	RaftProposeBatchLimit     int    `json:"raftProposeBatchLimit"`
	RaftMessageSendBatchLimit int64  `json:"raftMessageSendBatchLimit"`
	RaftMessageWorkerCount    uint64 `json:"raftMessageWorkerCount"`
	ApplyWorkerCount          uint64 `json:"applyWorkerCount"`

	CellCheckSizeDiff int64  `json:"cellCheckSizeDiff"`
	CellMaxSize       uint64 `json:"cellMaxSize"`
	CellSplitSize     uint64 `json:"cellSplitSize"`

	Raft *RaftCfg `json:"raft"`

	EnableRequestMetrics bool `json:"enableRequestMetrics"`
}

func (c *Cfg) getSnapDir() string {
	return fmt.Sprintf("%s/snap", c.StoreDataPath)
}

func (c *Cfg) getRaftGCLogDuration() time.Duration {
	return time.Duration(c.RaftGCLogIntervalMs) * time.Millisecond
}

func (c *Cfg) getStoreHeartbeatDuration() time.Duration {
	return time.Duration(c.StoreHeartbeatIntervalMs) * time.Millisecond
}

func (c *Cfg) getCellHeartbeatDuration() time.Duration {
	return time.Duration(c.CellHeartbeatIntervalMs) * time.Millisecond
}

func (c *Cfg) getReportCellDuration() time.Duration {
	return time.Duration(c.ReportCellIntervalMs) * time.Millisecond
}

func (c *Cfg) getMaxPeerDownSecDuration() time.Duration {
	return time.Duration(c.MaxPeerDownSec) * time.Second
}

func (c *Cfg) getSplitCellCheckDuration() time.Duration {
	return time.Duration(c.SplitCellCheckIntervalMs) * time.Millisecond
}

func (c *Cfg) getRaftBaseTickDuration() time.Duration {
	return time.Duration(c.Raft.BaseTick) * time.Millisecond
}

// RaftCfg is the cfg for raft
type RaftCfg struct {
	ElectionTick    int    `json:"electionTick"`
	HeartbeatTick   int    `json:"heartbeatTick"`
	MaxSizePerMsg   uint64 `json:"maxSizePerMsg"`
	MaxSizePerEntry uint64 `json:"maxSizePerEntry"`
	MaxInflightMsgs int    `json:"maxInflightMsgs"`
	BaseTick        int    `json:"baseTick"`
}
