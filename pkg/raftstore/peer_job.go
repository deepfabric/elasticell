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
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
)

func (ps *peerStorage) startApplyingSnapJob() {
	ps.applySnapJobLock.Lock()
	job, err := ps.store.addJob(ps.doApplyingSnapshotJob)
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: add apply snapshot task fail, errors:\n %+v",
			ps.cell.ID,
			err)
	}

	ps.applySnapJob = job
	ps.applySnapJobLock.Unlock()
}

func (ps *peerStorage) startDestroyDataJob(cellID uint64, start, end []byte) error {
	_, err := ps.store.addJob(func() error {
		return ps.doDestroyDataJob(cellID, start, end)
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
	if nil != ps.applySnapJob {
		ps.applySnapJob.Close()
		ps.applySnapJob = nil
	}
	ps.applySnapJobLock.Unlock()
}

func (ps *peerStorage) resetGenSnapJob() {
	if nil != ps.genSnapJob {
		ps.genSnapJob.Close()
		ps.genSnapJob = nil
	}
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

func (ps *peerStorage) doApplyingSnapshotJob() error {
	log.Infof("raftstore[cell-%d]: begin apply snap data", ps.cell.ID)

	localState, err := ps.loadCellLocalState(ps.applySnapJob)
	if err != nil {
		return err
	}

	err = ps.deleteAllInRange(encStartKey(localState.Cell), encEndKey(localState.Cell), ps.applySnapJob)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: apply snap delete range data failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return err
	}

	_, err = ps.loadSnapshot(ps.applySnapJob)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: apply snap load snapshot failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return err
	}

	// TODO: decode snapshot and set to local rocksdb.
	err = ps.updatePeerState(ps.cell, mraft.Normal)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: apply snap update peer state failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return err
	}

	log.Infof("raftstore[cell-%d]: apply snap complete", ps.cell.ID)
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

func (ps *peerStorage) isApplyingSnap() bool {
	return ps.applySnapJob != nil && ps.applySnapJob.IsNotComplete()
}

func (ps *peerStorage) isGeneratingSnap() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsNotComplete()
}

func (ps *peerStorage) isGenSnapJobComplete() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsComplete()
}
