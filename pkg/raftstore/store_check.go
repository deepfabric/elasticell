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
	"fmt"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/errorpb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
)

func (s *Store) isRaftMsgValid(msg *mraft.RaftMessage) bool {
	if msg.ToPeer.StoreID != s.id {
		log.Warnf("raftstore[store-%d]: store not match, toPeerStoreID=<%d> mineStoreID=<%d>",
			s.id,
			msg.ToPeer.StoreID,
			s.id)
		return false
	}

	return true
}

func (s *Store) isMsgStale(msg *mraft.RaftMessage) (bool, error) {
	cellID := msg.CellID
	fromEpoch := msg.CellEpoch
	isVoteMsg := msg.Message.Type == raftpb.MsgVote
	fromStoreID := msg.FromPeer.StoreID

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
	// tell 2 is stale, so 2 can remove itself.
	pr := s.getPeerReplicate(cellID)
	if nil != pr {
		c := pr.getCell()
		epoch := c.Epoch
		if isEpochStale(fromEpoch, epoch) &&
			findPeer(&c, fromStoreID) == nil {
			s.handleStaleMsg(msg, epoch, isVoteMsg)
			return true, nil
		}

		return false, nil
	}

	// no exist, check with tombstone key.
	localState, err := loadCellLocalState(cellID, s.engine, true)
	if err != nil {
		return false, err
	}

	if localState != nil {
		if localState.State != mraft.Tombstone {
			// Maybe split, but not registered yet.
			s.cacheDroppedVoteMsg(cellID, msg.Message)
			return false, fmt.Errorf("cell<%d> not exist but not tombstone, local state: %s",
				cellID,
				localState.String())
		}

		cellEpoch := localState.Cell.Epoch

		// The cell in this peer is already destroyed
		if isEpochStale(fromEpoch, cellEpoch) {
			log.Infof("raftstore: tombstone peer receive a a stale message, epoch=<%s> cell=<%d> msg=<%s>",
				cellEpoch.String(),
				cellID,
				msg.String())
			notExist := findPeer(&localState.Cell, fromStoreID) == nil
			s.handleStaleMsg(msg, cellEpoch, isVoteMsg && notExist)

			return true, nil
		}

		if fromEpoch.ConfVer == cellEpoch.ConfVer {
			return false, fmt.Errorf("tombstone peer receive an invalid message, epoch=<%s> msg=<%s>",
				cellEpoch.String(),
				msg.String())

		}
	}

	return false, nil
}

func (s *Store) checkSnapshot(msg *mraft.RaftMessage) (bool, error) {
	// Check if we can accept the snapshot
	pr := s.getPeerReplicate(msg.CellID)
	if len(msg.Message.Snapshot.Data) <= 0 {
		return true, nil
	}

	snap := msg.Message.Snapshot
	snapData := &mraft.SnapshotMessage{}
	err := snapData.Unmarshal(snap.Data)
	if err != nil {
		return false, err
	}

	if pr.getStore().isInitialized() {
		return true, nil
	}

	snapData.Header.FromPeer = msg.ToPeer
	snapData.Header.ToPeer = msg.FromPeer
	snapData.Ack = &mraft.SnapshotAckMessage{}

	snapCell := snapData.Header.Cell
	peerID := msg.ToPeer.ID

	found := false
	for _, p := range snapCell.Peers {
		if p.ID == peerID {
			found = true
			break
		}
	}

	if !found {
		log.Infof("raftstore[cell-%d]: cell doesn't contain peer, skip. cell=<%v> peer<%v>",
			snapCell.ID,
			snapCell,
			msg.ToPeer)
		snapData.Ack.Ack = mraft.Reject
		s.trans.sendSnapshotMessage(snapData)
		return false, nil
	}

	item := s.keyRanges.Search(snapCell.Start)
	if item.ID > 0 {
		exist := s.getPeerReplicate(item.ID).getCell()
		if bytes.Compare(encStartKey(&exist), encEndKey(&snapCell)) < 0 {
			log.Infof("cell overlapped, exist=<%v> target=<%v>",
				exist,
				snapCell)
			snapData.Ack.Ack = mraft.Reject
			s.trans.sendSnapshotMessage(snapData)
			return false, nil
		}
	}

	if !s.addPendingSnapshot(snapData.Header) {
		snapData.Ack.Ack = mraft.Reject
		s.trans.sendSnapshotMessage(snapData)
		return false, nil
	}

	return true, nil
}

func (s *Store) addPendingSnapshot(msg mraft.SnapshotMessageHeader) bool {
	s.pendingLock.Lock()
	if s.hasOverlapInPendingSnapshots(&msg.Cell) {
		s.pendingLock.Unlock()
		return false
	}

	s.pendingSnapshots[msg.Cell.ID] = msg
	s.pendingLock.Unlock()

	return true
}

func (s *Store) getPendingSnapshot(id uint64) mraft.SnapshotMessageHeader {
	s.pendingLock.RLock()
	value := s.pendingSnapshots[id]
	s.pendingLock.RUnlock()

	return value
}

func (s *Store) removePendingSnapshot(id uint64) {
	s.pendingLock.Lock()
	delete(s.pendingSnapshots, id)
	s.pendingLock.Unlock()
}

func (s *Store) hasOverlapInPendingSnapshots(cell *metapb.Cell) bool {
	for _, h := range s.pendingSnapshots {
		if HasOverlap(&h.Cell, cell) &&
			// Same cell can overlap, we will apply the latest version of snapshot.
			h.Cell.ID != cell.ID {
			log.Infof("pending cell overlapped, old=<%s> new=<%s>",
				h.Cell.String(),
				cell.String())
			return true
		}
	}
	return false
}

func checkEpoch(cell metapb.Cell, req *raftcmdpb.RaftCMDRequest) bool {
	checkVer := false
	checkConfVer := false

	if req.AdminRequest != nil {
		switch req.AdminRequest.Type {
		case raftcmdpb.Split:
			checkVer = true
		case raftcmdpb.ChangePeer:
			checkConfVer = true
		case raftcmdpb.TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	} else {
		// for redis command, we don't care conf version.
		checkConfVer = true
	}

	if !checkConfVer && !checkVer {
		return true
	}

	if req.Header == nil {
		return false
	}

	fromEpoch := req.Header.CellEpoch
	lastestEpoch := cell.Epoch

	if (checkConfVer && fromEpoch.ConfVer < lastestEpoch.ConfVer) ||
		(checkVer && fromEpoch.CellVer < lastestEpoch.CellVer) {
		log.Infof("check-epoch[cell-%d]: reveiced stale epoch, lastest=<%s> reveived=<%s>",
			cell.ID,
			lastestEpoch.String(),
			fromEpoch.String())
		return false
	}

	return true
}

func (s *Store) validateStoreID(req *raftcmdpb.RaftCMDRequest) error {
	if req.Header.Peer.StoreID != s.GetID() {
		return fmt.Errorf("store not match, give=<%d> want=<%d>",
			req.Header.Peer.StoreID,
			s.GetID())
	}

	return nil
}

func (s *Store) validateCell(req *raftcmdpb.RaftCMDRequest) *errorpb.Error {
	cellID := req.Header.CellId
	peerID := req.Header.Peer.ID

	pr := s.getPeerReplicate(cellID)
	if nil == pr {
		err := new(errorpb.CellNotFound)
		err.CellID = cellID

		return &errorpb.Error{
			Message:      errCellNotFound.Error(),
			CellNotFound: err,
		}
	}

	if !pr.isLeader() {
		err := new(errorpb.NotLeader)
		err.CellID = cellID
		err.Leader = s.getPeer(pr.getLeaderPeerID())

		return &errorpb.Error{
			Message:   errNotLeader.Error(),
			NotLeader: err,
		}
	}

	if pr.peer.ID != peerID {
		return &errorpb.Error{
			Message: fmt.Sprintf("mismatch peer id, give=<%d> want=<%d>", peerID, pr.peer.ID),
		}
	}

	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	if req.Header.Term > 0 && pr.getCurrentTerm() > req.Header.Term+1 {
		return &errorpb.Error{
			Message:      errStaleCMD.Error(),
			StaleCommand: infoStaleCMD,
		}
	}

	if !checkEpoch(pr.getCell(), req) {
		err := new(errorpb.StaleEpoch)
		// Attach the next cell which might be split from the current cell. But it doesn't
		// matter if the next cell is not split from the current cell. If the cell meta
		// received by the KV driver is newer than the meta cached in the driver, the meta is
		// updated.
		nextCell := s.keyRanges.NextCell(pr.getCell().Start)
		if nextCell != nil {
			err.NewCells = append(err.NewCells, *nextCell)
		}

		return &errorpb.Error{
			Message:    errStaleEpoch.Error(),
			StaleEpoch: err,
		}
	}

	return nil
}
