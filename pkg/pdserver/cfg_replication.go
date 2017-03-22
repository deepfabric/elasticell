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
	"sync"
)

// ReplicationCfg is the replication configuration.
type ReplicationCfg struct {
	sync.RWMutex
	// MaxReplicas is the number of replicas for each cell.
	MaxReplicas int `json:"maxReplicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels []string `json:"location-labels"`
}

func (c *Cfg) getMaxReplicas() int {
	c.Replication.RLock()
	defer c.Replication.RUnlock()

	return c.Replication.MaxReplicas
}

func (c *Cfg) getLocationLabels() []string {
	c.Replication.RLock()
	defer c.Replication.RUnlock()

	var value []string
	for _, v := range c.Replication.LocationLabels {
		value = append(value, v)
	}

	return value
}
