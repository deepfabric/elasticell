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

package node

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/storage/meta"
)

// Cfg node cfg
type Cfg struct {
	StoreAddr   string        `json:"storeAddr"`
	StoreLables []*meta.Label `json:"labels, omitempty"`

	PDEndpoints              []string `json:"pdRPCAddr"`
	StoreHeartbeatIntervalMs int      `json:"storeHeartbeatIntervalMs"`
	CellHeartbeatIntervalMs  int      `json:"cellHeartbeatIntervalMs"`
}

func (c *Cfg) getStoreHeartbeatDuration() time.Duration {
	return time.Duration(c.StoreHeartbeatIntervalMs) * time.Millisecond
}

func (c *Cfg) getCellHeartbeatDuration() time.Duration {
	return time.Duration(c.CellHeartbeatIntervalMs) * time.Millisecond
}
