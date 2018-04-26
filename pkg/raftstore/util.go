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
	"bytes"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/errorpb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
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

func findPeer(cell *metapb.Cell, storeID uint64) *metapb.Peer {
	for _, peer := range cell.Peers {
		if peer.StoreID == storeID {
			return peer
		}
	}

	return nil
}

func removePeer(cell *metapb.Cell, storeID uint64) {
	var newPeers []*metapb.Peer
	for _, peer := range cell.Peers {
		if peer.StoreID != storeID {
			newPeers = append(newPeers, peer)
		}
	}

	cell.Peers = newPeers
}

func newPeer(peerID, storeID uint64) metapb.Peer {
	return metapb.Peer{
		ID:      peerID,
		StoreID: storeID,
	}
}

func removedPeers(new, old metapb.Cell) []uint64 {
	var ids []uint64

	for _, o := range old.Peers {
		c := 0
		for _, n := range new.Peers {
			if n.ID == o.ID {
				c++
				break
			}
		}

		if c == 0 {
			ids = append(ids, o.ID)
		}
	}

	return ids
}

// Check if key in cell range [`startKey`, `endKey`).
func checkKeyInCell(key []byte, cell *metapb.Cell) *errorpb.Error {
	if bytes.Compare(key, cell.Start) >= 0 && (len(cell.End) == 0 || bytes.Compare(key, cell.End) < 0) {
		return nil
	}

	e := &errorpb.KeyNotInCell{
		Key:      key,
		CellID:   cell.ID,
		StartKey: cell.Start,
		EndKey:   cell.End,
	}

	return &errorpb.Error{
		Message:      errKeyNotInCell.Error(),
		KeyNotInCell: e,
	}
}

func newChangePeerRequest(changeType pdpb.ConfChangeType, peer metapb.Peer) *raftcmdpb.AdminRequest {
	req := new(raftcmdpb.AdminRequest)
	req.Type = raftcmdpb.ChangePeer

	subReq := new(raftcmdpb.ChangePeerRequest)
	subReq.ChangeType = changeType
	subReq.Peer = peer
	req.Body = util.MustMarshal(subReq)

	return req
}

func newTransferLeaderRequest(rsp *pdpb.TransferLeader) *raftcmdpb.AdminRequest {
	req := new(raftcmdpb.AdminRequest)
	req.Type = raftcmdpb.TransferLeader

	subReq := new(raftcmdpb.TransferLeaderRequest)
	subReq.Peer = rsp.Peer
	req.Body = util.MustMarshal(subReq)

	return req
}

func newCompactLogRequest(index, term uint64) *raftcmdpb.AdminRequest {
	req := new(raftcmdpb.AdminRequest)
	req.Type = raftcmdpb.RaftLogGC

	subReq := new(raftcmdpb.RaftLogGCRequest)
	subReq.CompactIndex = index
	subReq.CompactTerm = term
	req.Body = util.MustMarshal(subReq)

	return req
}

// SaveCell save  cell with state, raft state and apply state.
func SaveCell(driver storage.Driver, cell metapb.Cell) error {
	wb := driver.NewWriteBatch()

	// save state
	err := wb.Set(getCellStateKey(cell.ID), util.MustMarshal(&mraft.CellLocalState{Cell: cell}))
	if err != nil {
		return err
	}

	raftState := new(mraft.RaftLocalState)
	raftState.LastIndex = raftInitLogIndex
	raftState.HardState = raftpb.HardState{
		Term:   raftInitLogTerm,
		Commit: raftInitLogIndex,
	}

	err = wb.Set(getRaftStateKey(cell.ID), util.MustMarshal(raftState))
	if err != nil {
		return err
	}

	applyState := new(mraft.RaftApplyState)
	applyState.AppliedIndex = raftInitLogIndex
	applyState.TruncatedState = mraft.RaftTruncatedState{
		Term:  raftInitLogTerm,
		Index: raftInitLogIndex,
	}
	err = wb.Set(getApplyStateKey(cell.ID), util.MustMarshal(applyState))
	if err != nil {
		return err
	}

	log.Infof("bootstrap: begin to write first cell to local")
	return driver.Write(wb, false)
}

// DeleteCell delete cell with state, raft state and apply state.
func DeleteCell(id uint64, wb storage.WriteBatch) error {
	// save state
	err := wb.Delete(getCellStateKey(id))
	if err != nil {
		return err
	}

	err = wb.Delete(getRaftStateKey(id))
	if err != nil {
		return err
	}

	err = wb.Delete(getApplyStateKey(id))
	if err != nil {
		return err
	}

	return nil
}

// HasOverlap check cells has overlap
func HasOverlap(c1, c2 *metapb.Cell) bool {
	return bytes.Compare(encStartKey(c1), encEndKey(c2)) < 0 &&
		bytes.Compare(encEndKey(c1), encStartKey(c2)) > 0
}

// StalEpoch returns true if the target epoch is stale
func StalEpoch(target, check metapb.CellEpoch) bool {
	return (target.ConfVer < check.ConfVer) ||
		(target.CellVer < check.CellVer)
}
