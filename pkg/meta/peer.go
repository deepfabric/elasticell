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

// PeerDown is desc a peer how many seconds was down
type PeerDown struct {
	Peer        PeerMeta
	DownSeconds uint64
}

// PeerMeta is peer meta info
type PeerMeta struct {
	ID      uint64 `json:"id"`
	StoreID uint64 `json:"storeID"`
}

// Marshal marshal
func (m *PeerMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalPeerMeta returns the spec peer meta
func UnmarshalPeerMeta(data []byte) (*PeerMeta, error) {
	if data == nil {
		return nil, nil
	}

	s := new(PeerMeta)
	err := json.Unmarshal(data, s)
	return s, err
}

// Clone returns the clone value
func (m *PeerMeta) Clone() *PeerMeta {
	d, _ := m.Marshal()
	v, _ := UnmarshalPeerMeta(d)

	return v
}
