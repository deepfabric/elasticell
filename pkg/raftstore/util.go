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
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
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

// SaveFirstCell save first cell with state, raft state and apply state.
func SaveFirstCell(driver storage.Driver, cell metapb.Cell) error {
	// TODO: batch write

	// save state
	err := driver.Set(getCellStateKey(cell.ID), util.MustMarshal(&mraft.CellLocalState{Cell: cell}))
	if err != nil {
		return err
	}

	raftState := new(mraft.RaftLocalState)
	raftState.LastIndex = raftInitLogIndex
	raftState.HardState = raftpb.HardState{
		Term:   raftInitLogTerm,
		Commit: raftInitLogIndex,
	}
	err = driver.Set(getRaftStateKey(cell.ID), util.MustMarshal(raftState))
	if err != nil {
		return err
	}

	applyState := new(mraft.RaftApplyState)
	applyState.AppliedIndex = raftInitLogIndex
	applyState.TruncatedState = mraft.RaftTruncatedState{
		Term:  raftInitLogTerm,
		Index: raftInitLogIndex,
	}
	err = driver.Set(getApplyStateKey(cell.ID), util.MustMarshal(applyState))
	if err != nil {
		return err
	}

	return nil
}
