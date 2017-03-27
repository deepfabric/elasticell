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

package raft

import (
	ectdraft "github.com/coreos/etcd/raft"
	"github.com/deepfabric/elasticell/pkg/storage"
)

type Raft struct {
	cfg   *Cfg
	store ectdraft.Storage
	node  *ectdraft.Node
}

func NewRaft(cfg *Cfg, driver *storage.Driver) *Raft {
	r := new(Raft)
	r.cfg = cfg
	// r.node = ectdraft.Re
	return nil
}

func getRaftConfig(id, appliedIndex uint64, store ectdraft.Storage) *ectdraft.Config {
	return &ectdraft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         store,
		MaxSizePerMsg:   16 * 1024,
		MaxInflightMsgs: 64,
	}
}
