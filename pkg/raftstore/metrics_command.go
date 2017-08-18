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
	labelCommandAdminPerAll     = "all"
	labelCommandAdminConfChange = "conf_change"
	labelCommandAdminAddPeer    = "add_peer"
	labelCommandAdminRemovePeer = "remove_peer"
	labelCommandAdminSplit      = "split"
	labelCommandAdminCompact    = "compact"

	labelCommandAdminSucceed      = "succeed"
	labelCommandAdminRejectUnsafe = "reject_unsafe"
)

// metrics for command
var (
	commandCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "command_normal_total",
			Help:      "Total number of normal commands received.",
		}, []string{"type"})

	commandAdminCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "elasticell",
			Subsystem: "cell",
			Name:      "command_admin_total",
			Help:      "Total number of admin commands processed.",
		}, []string{"type", "status"})
)

func initMetricsForCommand() {
	prometheus.MustRegister(commandCounterVec)
	prometheus.MustRegister(commandAdminCounterVec)
}
