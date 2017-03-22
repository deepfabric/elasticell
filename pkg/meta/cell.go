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
	"sync"
)

// CellEpoch cell epoch verison
// ConfVer auto increment when add or remove peer
// CellVer auto increment when split or merge
type CellEpoch struct {
	ConfVer int64
	CellVer int64
}

// CellMeta is cell meta info
type CellMeta struct {
	sync.Mutex

	ID    uint64      `json:"id"`
	Min   []byte      `json:"min"`
	Max   []byte      `json:"max"`
	Epoch *CellEpoch  `json:"epoch"`
	Peers []*PeerMeta `json:"peers"`
}

// NewCellMeta returns a cell meta
func NewCellMeta(id, storeID uint64) *CellMeta {
	c := &CellMeta{
		ID:    id,
		Epoch: newCellEpoch(),
	}

	c.AddPeer(&PeerMeta{
		ID:      id,
		StoreID: storeID,
	})
	return c
}

func newCellEpoch() *CellEpoch {
	return &CellEpoch{
		ConfVer: 1,
		CellVer: 1,
	}
}

// Marshal marshal
func (m *CellMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalCellMeta returns the spec cell meta
func UnmarshalCellMeta(data []byte) (*CellMeta, error) {
	if data == nil {
		return nil, nil
	}

	s := new(CellMeta)
	err := json.Unmarshal(data, s)
	return s, err
}

// AddPeer add a peer
func (m *CellMeta) AddPeer(peer *PeerMeta) {
	m.Lock()
	m.Unlock()

	m.Peers = append(m.Peers, peer)
}

// Clone returns the clone value
func (m *CellMeta) Clone() *CellMeta {
	d, _ := m.Marshal()
	v, _ := UnmarshalCellMeta(d)

	return v
}
