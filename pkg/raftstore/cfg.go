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

import "time"

// Cfg for raftstore
type Cfg struct {
	StoreHeartbeatIntervalMs int      `json:"storeHeartbeatIntervalMs"`
	CellHeartbeatIntervalMs  int      `json:"cellHeartbeatIntervalMs"`
	MaxPeerDownSec           int      `json:"maxPeerDownSec"`
	Raft                     *RaftCfg `json:"raft"`
}

func (c *Cfg) getStoreHeartbeatDuration() time.Duration {
	return time.Duration(c.StoreHeartbeatIntervalMs) * time.Millisecond
}

func (c *Cfg) getCellHeartbeatDuration() time.Duration {
	return time.Duration(c.CellHeartbeatIntervalMs) * time.Millisecond
}

func (c *Cfg) getMaxPeerDownSecDuration() time.Duration {
	return time.Duration(c.MaxPeerDownSec) * time.Second
}

// RaftCfg is the cfg for raft
type RaftCfg struct {
	PeerAddr          string
	PeerAdvertiseAddr string
	ElectionTick      int
	HeartbeatTick     int
	MaxSizePerMsg     uint64
	MaxInflightMsgs   int
	SnapDir           string
	BaseTick          int
}
