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

package pdapi

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"regexp"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/pkg/errors"
)

// SetLogLevel set log level for components
type SetLogLevel struct {
	Targets []uint64 `json:"targets"`
	Level   int32    `json:"level"`
}

// StoreStatus store status
type StoreStatus struct {
	Stats           *pdpb.StoreStats `json:"stats"`
	LeaderCount     uint32           `json:"leaderCount"`
	LastHeartbeatTS int64            `json:"lastHeartbeatTS"`
}

// StoreInfo store info
type StoreInfo struct {
	Meta   metapb.Store `json:"meta"`
	Status *StoreStatus `json:"status"`
}

// CellInfo The cell info
type CellInfo struct {
	Meta         metapb.Cell      `json:"meta"`
	LeaderPeer   *metapb.Peer     `json:"leader"`
	DownPeers    []pdpb.PeerStats `json:"downPeers"`
	PendingPeers []metapb.Peer    `json:"pendingPeers"`
}

// System The system info of the elasticell cluster
type System struct {
	AlreadyBootstrapped bool        `json:"alreadyBootstrapped"`
	InitParams          *InitParams `json:"initParams, omitempty"`

	MaxReplicas uint32 `json:"maxReplicas, omitempty"`

	StoreCount          int `json:"storeCount, omitempty"`
	OfflineStoreCount   int `json:"offlineStoreCount, omitempty"`
	TombStoneStoreCount int `json:"tombStoneStoreCount, omitempty"`

	CellCount                int `json:"cellCount, omitempty"`
	ReplicasNotFullCellCount int `json:"replicasNotFullCellCount, omitempty"`

	StorageCapacity  uint64 `json:"storageCapacity, omitempty"`
	StorageAvailable uint64 `json:"storageAvailable, omitempty"`

	OperatorCount int `json:"operatorCount, omitempty"`
}

// InitParams initialize the elasticell cluster
type InitParams struct {
	InitCellCount uint64 `json:"initCellCount"`
	CellCapacity  uint64 `json:"cellCapacity"`
}

// Marshal marshal
func (p *InitParams) Marshal() (string, error) {
	v, err := json.Marshal(p)
	if err != nil {
		return "", err
	}

	return string(v), nil
}

// Service service interface
type Service interface {
	Name() string
	IsLeader() bool
	GetLeader() (*pdpb.Leader, error)

	GetSystem() (*System, error)
	InitCluster(params *InitParams) error

	ListStore() ([]*StoreInfo, error)
	GetStore(id uint64) (*StoreInfo, error)
	DeleteStore(id uint64, force bool) error
	SetStoreLogLevel(set *SetLogLevel) error

	ListCellInStore(storeID uint64) ([]*CellInfo, error)
	ListCell() ([]*CellInfo, error)
	GetCell(id uint64) (*CellInfo, error)
	TransferLeader(transfer *TransferLeader) error
	GetOperator(id uint64) (interface{}, error)

	GetOperators() ([]interface{}, error)

	ListIndex() ([]*pdpb.IndexDef, error)
	GetIndex(id string) (*pdpb.IndexDef, error)
	CreateIndex(idxDef *pdpb.IndexDef) error
	DeleteIndex(id string) error
}

// TransferLeader transfer leader to spec peer
type TransferLeader struct {
	CellID   uint64 `json:"cellId"`
	ToPeerID uint64 `json:"toPeerId"`
}

func readTransferLeader(r io.ReadCloser) (*TransferLeader, error) {
	value := &TransferLeader{}
	return value, readJSON(r, value)
}

func readSetLogLevel(r io.ReadCloser) (*SetLogLevel, error) {
	value := &SetLogLevel{}
	return value, readJSON(r, value)
}

func readInitParams(r io.ReadCloser) (*InitParams, error) {
	value := &InitParams{}
	return value, readJSON(r, value)
}

func readIndexDef(r io.ReadCloser) (value *pdpb.IndexDef, err error) {
	value = &pdpb.IndexDef{}
	if err = readJSON(r, value); err != nil {
		return
	}
	//input validation
	if _, err = regexp.Compile(value.KeyPattern); err != nil {
		return
	}
	return
}

func readJSON(r io.ReadCloser, data interface{}) error {
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "")
	}
	err = json.Unmarshal(b, data)
	if err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
