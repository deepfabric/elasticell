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

// Cfg pd server Cfg
type Cfg struct {
	Name                     string
	DataPath                 string
	AddrRPC                  string
	DurationLeaderLease      int64
	DurationHeartbeatWatcher time.Duration
	ThresholdPauseWatcher    int

	URLsClient          string
	URLsAdvertiseClient string
	URLsPeer            string
	URLsAdvertisePeer   string
	InitialCluster      string
	InitialClusterState string

	// The label keys specified the location of a store.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LabelsLocation []string
	// LimitReplicas is the number of replicas for each cell.
	LimitReplicas uint32
	// If the snapshot count of one store is greater than this value,
	// it will never be used as a source or target store.
	LimitSnapshots uint64
	// MaxStoreDownTime is the max duration after which
	// a store will be considered to be down if it hasn't reported heartbeats.
	LimitStoreDownDuration time.Duration
	// LimitScheduleLeader is the max coexist leader schedules.
	LimitScheduleLeader uint64
	// LimitScheduleCell is the max coexist cell schedules.
	LimitScheduleCell uint64
	// LimitScheduleReplica is the max coexist replica schedules.
	LimitScheduleReplica uint64
	// ThresholdStorageRate is the max storage rate of used for schduler
	ThresholdStorageRate int
}
