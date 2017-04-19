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
	"fmt"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
)

func (pr *PeerReplicate) startApplyingSnapJob() {
	pr.ps.applySnapJobLock.Lock()
	job, err := pr.store.addJob(pr.doApplyingSnapshotJob)
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: add apply snapshot task fail, errors:\n %+v",
			pr.cellID,
			err)
	}

	pr.ps.applySnapJob = job
	pr.ps.applySnapJobLock.Unlock()
}

func (ps *peerStorage) startDestroyDataJob(cellID uint64, start, end []byte) error {
	_, err := ps.store.addJob(func() error {
		return ps.doDestroyDataJob(cellID, start, end)
	})

	return err
}

func (pr *PeerReplicate) startRegistrationJob() {
	delegate := &applyDelegate{
		store:            pr.store,
		peerID:           pr.peer.ID,
		cell:             pr.ps.cell,
		term:             pr.getCurrentTerm(),
		applyState:       *pr.ps.getApplyState(),
		appliedIndexTerm: pr.ps.getAppliedIndexTerm(),
	}
	_, err := pr.store.addJob(func() error {
		return pr.doRegistrationJob(delegate)
	})

	if err != nil {
		log.Fatalf("raftstore[cell-%d]: add registration job failed, errors:\n %+v",
			pr.ps.cell.ID,
			err)
	}
}

func (pr *PeerReplicate) startApplyCommittedEntriesJob(cellID uint64, term uint64, commitedEntries []raftpb.Entry) error {
	_, err := pr.store.addJob(func() error {
		return pr.doApplyCommittedEntries(cellID, term, commitedEntries)
	})

	return err
}

func (ps *peerStorage) cancelApplyingSnapJob() bool {
	ps.applySnapJobLock.RLock()
	if ps.applySnapJob == nil {
		ps.applySnapJobLock.RUnlock()
		return true
	}

	ps.applySnapJob.Cancel()

	if ps.applySnapJob.IsCancelled() {
		ps.applySnapJobLock.RUnlock()
		return true
	}

	succ := !ps.isApplyingSnap()
	ps.applySnapJobLock.RUnlock()
	return succ
}

func (ps *peerStorage) resetApplyingSnapJob() {
	ps.applySnapJobLock.Lock()
	ps.applySnapJob = nil
	ps.applySnapJobLock.Unlock()
}

func (ps *peerStorage) resetGenSnapJob() {
	ps.genSnapJob = nil
	ps.snapTriedCnt = 0
}

func (ps *peerStorage) doDestroyDataJob(cellID uint64, startKey, endKey []byte) error {
	log.Infof("raftstore[cell-%d]: deleting data, start=<%v>, end=<%v>",
		cellID,
		startKey,
		endKey)
	// TODO: imple

	return nil
}

func (pr *PeerReplicate) doApplyingSnapshotJob() error {
	log.Infof("raftstore[cell-%d]: begin apply snap data", pr.cellID)
	defer pr.rn.Advance()

	localState, err := pr.ps.loadCellLocalState(pr.ps.applySnapJob)
	if err != nil {
		return err
	}

	err = pr.ps.deleteAllInRange(encStartKey(&localState.Cell), encEndKey(&localState.Cell), pr.ps.applySnapJob)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: apply snap delete range data failed, errors:\n %+v",
			pr.cellID,
			err)
		return err
	}

	_, err = pr.ps.loadSnapshot(pr.ps.applySnapJob)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: apply snap load snapshot failed, errors:\n %+v",
			pr.cellID,
			err)
		return err
	}

	// TODO: decode snapshot and set to local rocksdb.
	err = pr.ps.updatePeerState(pr.ps.cell, mraft.Normal)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: apply snap update peer state failed, errors:\n %+v",
			pr.cellID,
			err)
		return err
	}

	log.Infof("raftstore[cell-%d]: apply snap complete", pr.cellID)
	return nil
}

func (ps *peerStorage) doGenerateSnapshotJob() error {
	if ps.genSnapJob == nil {
		log.Fatalf("raftstore[cell-%d]: generating snapshot job chan is nil", ps.cell.ID)
	}

	applyState, err := ps.loadApplyState()
	if err != nil {
		log.Errorf("raftstore[cell-%d]: load snapshot failure, errors:\n %+v",
			ps.cell.ID,
			err)
		return nil
	} else if nil == applyState {
		log.Errorf("raftstore[cell-%d]: could not load snapshot", ps.cell.ID)
		return nil
	}

	var term uint64
	if applyState.AppliedIndex == applyState.TruncatedState.Index {
		term = applyState.TruncatedState.Term
	} else {
		entry, err := ps.loadLogEntry(applyState.AppliedIndex)
		if err != nil {
			return nil
		}

		term = entry.Term
	}

	state, err := ps.loadCellLocalState(nil)
	if err != nil {
		return nil
	}

	if state.State != mraft.Normal {
		log.Errorf("raftstore[cell-%d]: snap seems stale, skip", ps.cell.ID)
		return nil
	}

	snapshot := raftpb.Snapshot{}
	snapshot.Metadata.Term = term
	snapshot.Metadata.Index = applyState.AppliedIndex

	confState := raftpb.ConfState{}
	for _, peer := range ps.cell.Peers {
		confState.Nodes = append(confState.Nodes, peer.ID)
	}
	snapshot.Metadata.ConfState = confState

	// snapData := mraft.RaftSnapshotData{}
	// snapData.Cell = state.Cell
	// snapData.Data = nil

	// d, err := snapData.Marshal()
	// if err != nil {
	// 	log.Errorf("raftstore[cell-%d]: snapshot failure, errors:\n %+v",
	// 		ps.cell.ID,
	// 		err)
	// 	ps.snapStateC <- nil
	// 	return nil
	// }

	// TODO: impl snapshot data load from rocksdb and compact
	// snapshot.Data = d

	err = ps.snapshortter.SaveSnap(snapshot)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: snapshot failure, errors:\n %+v",
			ps.cell.ID,
			err)
	} else {
		log.Infof("raftstore[cell-%d]: snapshot complete", ps.cell.ID)
		ps.genSnapJob.SetResult(snapshot)
	}

	return nil
}

func (pr *PeerReplicate) doRegistrationJob(delegate *applyDelegate) error {
	old := pr.store.delegates.put(delegate.cell.ID, delegate)
	if old != nil {
		if old.peerID != delegate.peerID {
			log.Fatalf("raftstore[cell-%d]: delegate peer id not match, old=<%d> curr=<%d>",
				pr.cellID,
				old.peerID,
				delegate.peerID)
		}

		old.term = delegate.term
		old.clearAllCommandsAsStale()
	}

	return nil
}

func (pr *PeerReplicate) doApplyCommittedEntries(cellID uint64, term uint64, commitedEntries []raftpb.Entry) error {
	defer pr.rn.Advance()

	delegate := pr.store.delegates.get(cellID)
	if nil == delegate {
		return fmt.Errorf("raftstore[cell-%d]: missing delegate", pr.cellID)
	}

	delegate.term = term

	delegate.applyCommittedEntries(commitedEntries)

	if delegate.isPendingRemove() {
		delegate.destroy()
	}

	// TODO: impl handle result

	if delegate.isPendingRemove() {
		pr.store.delegates.delete(delegate.cell.ID)
	}
	return nil
}

func (ps *peerStorage) isApplyingSnap() bool {
	return ps.applySnapJob != nil && ps.applySnapJob.IsNotComplete()
}

func (ps *peerStorage) isGeneratingSnap() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsNotComplete()
}

func (ps *peerStorage) isGenSnapJobComplete() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsComplete()
}
