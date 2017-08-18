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
	"github.com/prometheus/client_golang/prometheus"
)

const (
	labelStoreStorageCapacity  = "capacity"
	labelStoreStorageAvailable = "available"

	labelStoreCellLeader = "leader"
)

var (
	storeCellCountGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "store_cell_total",
			Help:      "Total number of store cells.",
		}, []string{"type"})

	storeStorageGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "store_storage_bytes",
			Help:      "Size of raftstore storage.",
		}, []string{"type"})

	storeWrittenBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "store_written_bytes",
			Help:      "Histogram of bytes written for cells.",
			Buckets:   prometheus.ExponentialBuckets(256.0, 2.0, 20),
		})

	storeWrittenKeysHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "store_written_keys",
			Help:      "Histogram of keys written for cells.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 20),
		})
)

func initMetricsForStore() {
	prometheus.MustRegister(storeCellCountGaugeVec)
	prometheus.MustRegister(storeStorageGaugeVec)
	prometheus.MustRegister(storeWrittenBytesHistogram)
	prometheus.MustRegister(storeWrittenKeysHistogram)
}
