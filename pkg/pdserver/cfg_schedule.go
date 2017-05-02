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

// ScheduleConfig is the schedule configuration.
type ScheduleConfig struct {
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	MaxSnapshotCount uint64 `json:"max-snapshot-count"`
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	MaxStoreDownTimeMs int `json:"max-store-down-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `json:"leader-schedule-limit"`
	// CellScheduleLimit is the max coexist cell schedules.
	CellScheduleLimit uint64 `json:"cell-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `json:"replica-schedule-limit"`
}

func (c *ScheduleConfig) getMaxStoreDownTimeDuration() time.Duration {
	return time.Duration(c.MaxStoreDownTimeMs) * time.Millisecond
}
