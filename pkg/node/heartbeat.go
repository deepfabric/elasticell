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

package node

import (
	"github.com/deepfabric/elasticell/pkg/log"
)

func (n *Node) startHeartbeat() {
	// That's ok, the cluster is bootstrapped succ.
	// We will start 1+N heartbeat loop to report store and cell info to pd.
	if nil != n.storeHeartbeat {
		go n.storeHeartbeat.start()
	}

	// N cell loop
	for _, h := range n.cellHeartbeats {
		go h.start()
	}
}

func (n *Node) stopHeartbeat() {
	for _, h := range n.cellHeartbeats {
		h.stop()
	}

	if nil != n.storeHeartbeat {
		n.storeHeartbeat.stop()
	}

	log.Infof("stop: all heartbeats are stopped")
}

func (n *Node) doStoreHeartbeat(storeID uint64) {

}

func (n *Node) doCellHeartbeat(cellID uint64) {

}
