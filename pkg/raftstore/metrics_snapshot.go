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

package raftstore

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	labelSnapshotActionSent     = "sent"
	labelSnapshotActionReceived = "received"
	labelSnapshotActionApplying = "applying"
)

// metrics for snapshot
var (
	snapshotSendingDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "snapshot_sending_duration_seconds",
			Help:      "Bucketed histogram of server send snapshots duration.",
		})

	snapshotBuildingDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "snapshot_building_duration_seconds",
			Help:      "Bucketed histogram of snapshot build time duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	snapshotSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "snapshot_size_bytes",
			Help:      "Bytes of per snapshot.",
			Buckets:   prometheus.ExponentialBuckets(1024.0, 2.0, 22),
		})

	snapshortActionGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "snapshot_action_total",
			Help:      "Total number of raftstore snapshot action.",
		}, []string{"type"})
)

func initMetricsForSnapshot() {
	prometheus.MustRegister(snapshotBuildingDurationHistogram)
	prometheus.MustRegister(snapshortActionGaugeVec)
	prometheus.MustRegister(snapshotSizeHistogram)
	prometheus.MustRegister(snapshotSendingDurationHistogram)
}

func observeSnapshotBuild(start time.Time) {
	snapshotBuildingDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

func observeSnapshotSending(start time.Time) {
	snapshotSendingDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}
