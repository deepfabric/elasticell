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
	"time"
)

// ScheduleCfg is the schedule configuration.
type ScheduleCfg struct {
	// MaxReplicas is the number of replicas for each cell.
	MaxReplicas uint32 `json:"maxReplicas"`
	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels []string `json:"locationLabels"`
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or targCet store.
	MaxSnapshotCount uint64 `json:"maxSnapshotCount"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownTimeMs int `json:"maxStoreDownTimeMs"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `json:"leaderScheduleLimit"`
	// CellScheduleLimit is the max coexist cell schedules.
	CellScheduleLimit uint64 `json:"cellScheduleLimit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `json:"replicaScheduleLimit"`
	// StorageRatioThreshold is the max storage rate of used for schduler
	StorageRatioThreshold int `json:"storageRatioThreshold"`
}

func (c *ScheduleCfg) getMaxStoreDownTimeDuration() time.Duration {
	return time.Duration(c.MaxStoreDownTimeMs) * time.Millisecond
}
