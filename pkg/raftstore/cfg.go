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
	Addr                      string
	DataPath                  string
	OptionPath                string
	CellCapacity              uint64
	DurationHeartbeatStore    time.Duration
	DurationHeartbeatCell     time.Duration
	DurationSplitCheck        time.Duration
	DurationCompact           time.Duration
	DurationReportMetric      time.Duration
	DurationRaftTick          time.Duration
	DurationRetrySentSnapshot time.Duration
	LimitPeerDownDuration     time.Duration
	LimitCompactCount         uint64
	LimitCompactBytes         uint64
	LimitCompactLag           uint64
	LimitRaftMsgCount         int
	LimitRaftMsgBytes         uint64
	LimitRaftEntryBytes       uint64
	LimitSnapChunkBytes       uint64
	LimitSnapChunkRate        uint64
	LimitConcurrencyWrite     uint64
	ThresholdCompact          uint64
	ThresholdSplitCheckBytes  uint64
	ThresholdRaftElection     int
	ThresholdRaftHeartbeat    int
	BatchSizeProposal         uint64
	BatchSizeSent             uint64
	WorkerCountSent           uint64
	WorkerCountSentSnap       uint64
	WorkerCountApply          uint64
	EnableMetricsRequest      bool
}

// NewCfg return default cfg
func NewCfg() *Cfg {
	return &Cfg{}
}

func (c *Cfg) getSnapDir() string {
	return fmt.Sprintf("%s/%s", c.DataPath, getSnapDirName())
}

func getSnapDirName() string {
	return "snap"
}
