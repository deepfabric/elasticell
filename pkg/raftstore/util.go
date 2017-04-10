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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

const (
	invalidIndex = 0
)

// check whether epoch is staler than checkEpoch.
func isEpochStale(epoch metapb.CellEpoch, checkEpoch metapb.CellEpoch) bool {
	return epoch.CellVer < checkEpoch.CellVer ||
		epoch.ConfVer < checkEpoch.ConfVer
}

func findPeer(cell metapb.Cell, storeID uint64) *metapb.Peer {
	for _, peer := range cell.Peers {
		if peer.StoreID == storeID {
			return peer
		}
	}

	return nil
}

func newPeer(peerID, storeID uint64) metapb.Peer {
	return metapb.Peer{
		ID:      peerID,
		StoreID: storeID,
	}
}
