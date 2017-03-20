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

// ClusterMeta cluster meta info
type ClusterMeta struct {
	ID          uint64
	MaxReplicas int
}

// NewClusterMeta returns cluster meta use spec id and maxReplicas
func NewClusterMeta(id uint64, maxReplicas int) *ClusterMeta {
	return &ClusterMeta{
		ID:          id,
		MaxReplicas: maxReplicas,
	}
}

// Marshal marshal
func (m *ClusterMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalClusterMeta returns the spec cluster meta
func UnmarshalClusterMeta(data []byte) (*ClusterMeta, error) {
	s := new(ClusterMeta)
	err := json.Unmarshal(data, s)
	return s, err
}
