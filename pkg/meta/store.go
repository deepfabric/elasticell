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

package meta

import (
	"encoding/json"
)

// StoreState used for desc store state
type StoreState int

const (
	// StoreStateUp state is normal
	StoreStateUp = 0
	// StoreStateOffline state is offline, may be some time return to down state.
	StoreStateOffline = 1
	// StoreStateDown state is down
	StoreStateDown = 2
)

// StoreMetrics is the store some metrix used for scheduler
type StoreMetrics struct {
	// Capacity for the store.
	Capacity uint64 `json:"capacity"`
	// Available size for the store.
	Available uint64 `json:"available"`
	// CellCount total cell count in this store.
	CellCount int `json:"cellCount"`
}

// StoreMeta store meta info
type StoreMeta struct {
	ID      uint64        `json:"id"`
	Address string        `json:"address"`
	Lables  []*Label      `json:"labels"`
	State   StoreState    `json:"state"`
	Metrics *StoreMetrics `json:"metrics"`
}

// NewStoreMeta returns a store meta
func NewStoreMeta() *StoreMeta {
	return &StoreMeta{}
}

// Marshal marshal
func (m *StoreMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalStoreMeta returns the spec store meta
func UnmarshalStoreMeta(data []byte) (*StoreMeta, error) {
	s := new(StoreMeta)
	err := json.Unmarshal(data, s)
	return s, err
}

// Clone returns the clone value
func (m *StoreMeta) Clone() *StoreMeta {
	d, _ := m.Marshal()
	v, _ := UnmarshalStoreMeta(d)

	return v
}
