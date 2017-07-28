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
	"math"
	"sync"
)

// ReplicationCfg is the replication configuration.
type ReplicationCfg struct {
	sync.RWMutex
	// MaxReplicas is the number of replicas for each cell.
	MaxReplicas uint32 `json:"maxReplicas"`

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels []string `json:"location-labels"`
}

func (c *Cfg) getLocationLabels() []string {
	var value []string
	for _, v := range c.Schedule.LocationLabels {
		value = append(value, v)
	}

	return value
}

// getDistinctScore returns the score that the other is distinct from the stores.
// A higher score means the other store is more different from the existed stores.
func (c *Cfg) getDistinctScore(stores []*StoreInfo, other *StoreInfo) float64 {
	score := float64(0)
	locationLabels := c.getLocationLabels()

	for i := range locationLabels {
		keys := locationLabels[0 : i+1]
		level := len(locationLabels) - i - 1
		levelScore := math.Pow(replicaBaseScore, float64(level))

		for _, s := range stores {
			if s.getID() == other.getID() {
				continue
			}
			id1 := s.getLocationID(keys)
			if len(id1) == 0 {
				return 0
			}
			id2 := other.getLocationID(keys)
			if len(id2) == 0 {
				return 0
			}
			if id1 != id2 {
				score += levelScore
			}
		}
	}

	return score
}
