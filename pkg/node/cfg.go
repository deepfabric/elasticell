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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/raftstore"
)

// Cfg node cfg
type Cfg struct {
	ClusterID          uint64         `json:"clusterID"`
	StoreAddr          string         `json:"storeAddr"`
	StoreAdvertiseAddr string         `json:"storeAdvertiseAddr"`
	StoreLables        []metapb.Label `json:"labels, omitempty"`

	PDEndpoints []string       `json:"pdRPCAddr"`
	RaftStore   *raftstore.Cfg `json:"raftStore"`
}
