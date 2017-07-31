// Copyright 2016 PingCAP, Inc.
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

package pdapi

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

// StoreStatus store status
type StoreStatus struct {
	Stats           *pdpb.StoreStats `json:"stats"`
	LeaderCount     uint32           `json:"leaderCount"`
	LastHeartbeatTS time.Time        `json:"lastHeartbeatTS"`
}

// StoreInfo store info
type StoreInfo struct {
	Meta   metapb.Store `json:"meta"`
	Status *StoreStatus `json:"status"`
}

// Service service interface
type Service interface {
	Name() string
	IsLeader() bool
	GetLeader() (*pdpb.Leader, error)

	ListStore() ([]*StoreInfo, error)
	GetStore(id uint64) (*StoreInfo, error)
	DeleteStore(id uint64, force bool) error
}
