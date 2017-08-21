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

package pdserver

import (
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

type storeOperatorKind int

const (
	setLogLevelKind storeOperatorKind = iota
)

// StoreOperator is an interface to operate store
type StoreOperator interface {
	GetStoreID() uint64
	Do(store *StoreInfo) (*pdpb.StoreHeartbeatRsp, bool)
}

func newSetLogLevelOperator(id uint64, newLevel int32) StoreOperator {
	return &setLogLevelOperator{
		id:       id,
		newLevel: newLevel,
	}
}

type setLogLevelOperator struct {
	id       uint64
	newLevel int32
}

func (op *setLogLevelOperator) GetStoreID() uint64 {
	return op.id
}

func (op *setLogLevelOperator) Do(store *StoreInfo) (*pdpb.StoreHeartbeatRsp, bool) {
	if store.Status.Stats.LogLevel == op.newLevel {
		return nil, true
	}

	return &pdpb.StoreHeartbeatRsp{
		SetLogLevel: &pdpb.SetLogLevel{
			NewLevel: op.newLevel,
		},
	}, false
}
