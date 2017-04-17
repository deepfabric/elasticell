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
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

const (
	// When we create a region peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	raftInitLogTerm  = 5
	raftInitLogIndex = 5

	maxSnapTryCnt = 5
)

type snapshotState int

var (
	relax        = snapshotState(1)
	generating   = snapshotState(2)
	applying     = snapshotState(3)
	applyAborted = snapshotState(4)
)

const (
	pending = iota
	running
	cancelling
	cancelled
	finished
	failed
)

type peerStorage struct {
	store            *Store
	cell             metapb.Cell
	lastTerm         uint64
	appliedIndexTerm uint64
	snapshortter     *snap.Snapshotter
	raftState        mraft.RaftLocalState
	applyState       mraft.RaftApplyState

	snapTriedCnt     int
	genSnapJob       *util.Job
	applySnapJob     *util.Job
	applySnapJobLock sync.RWMutex

	pendingReads *readIndexQueue
}

func newPeerStorage(store *Store, cell metapb.Cell) (*peerStorage, error) {
	s := new(peerStorage)
	s.store = store
	s.cell = cell
	s.appliedIndexTerm = raftInitLogTerm

	err := s.initRaftState()
	if err != nil {
		return nil, err
	}
	err = s.initApplyState()
	if err != nil {
		return nil, err
	}
	err = s.initLastTerm()
	if err != nil {
		return nil, err
	}

	s.snapshortter = snap.New(fmt.Sprintf("%s/%d", store.cfg.Raft.SnapDir, s.cell.ID))
	s.pendingReads = new(readIndexQueue)

	return s, nil
}

func (ps *peerStorage) initRaftState() error {
	v, err := ps.store.engine.Get(getRaftStateKey(ps.cell.ID))
	if err != nil {
		return errors.Wrap(err, "")
	}

	s := &mraft.RaftLocalState{}
	err = s.Unmarshal(v)
	if err != nil {
		return errors.Wrap(err, "")
	}

	if len(ps.cell.Peers) > 0 {
		s.LastIndex = raftInitLogIndex
	}

	ps.raftState = *s
	return nil
}

func (ps *peerStorage) initApplyState() error {
	v, err := ps.store.engine.Get(getApplyStateKey(ps.cell.ID))
	if err != nil {
		return errors.Wrap(err, "")
	}

	if nil == v && len(ps.cell.Peers) > 0 {
		ps.applyState.AppliedIndex = raftInitLogIndex
		ps.applyState.TruncatedState.Index = raftInitLogIndex
		ps.applyState.TruncatedState.Term = raftInitLogTerm
		return nil
	}

	s := &mraft.RaftApplyState{}
	err = s.Unmarshal(v)
	if err != nil {
		return errors.Wrap(err, "")
	}

	ps.applyState = *s
	return nil
}

func (ps *peerStorage) initLastTerm() error {
	lastIndex := ps.raftState.LastIndex

	if lastIndex == 0 {
		ps.lastTerm = lastIndex
		return nil
	} else if lastIndex == raftInitLogIndex {
		ps.lastTerm = raftInitLogTerm
		return nil
	} else if lastIndex == ps.applyState.TruncatedState.Index {
		ps.lastTerm = ps.applyState.TruncatedState.Term
		return nil
	} else if lastIndex < raftInitLogIndex {
		log.Fatalf("raftstore[cell-%d]: error raft last index, index=<%d>",
			ps.cell.ID,
			lastIndex)
		return nil
	}

	v, err := ps.store.engine.Get(getRaftLogKey(ps.cell.ID, lastIndex))
	if err != nil {
		return errors.Wrap(err, "")
	}

	if nil == v {
		return fmt.Errorf("raftstore[cell-%d]: entry at index<%d> doesn't exist, may lose data",
			ps.cell.ID,
			lastIndex)
	}

	s := &raftpb.Entry{}
	err = s.Unmarshal(v)
	if err != nil {
		return errors.Wrap(err, "")
	}

	ps.lastTerm = s.Term
	return nil
}

func (ps *peerStorage) isApplyComplete() bool {
	return ps.raftState.HardState.Commit == ps.getAppliedIndex()
}

func (ps *peerStorage) setApplyState(applyState *mraft.RaftApplyState) {
	ps.applyState = *applyState
}

func (ps *peerStorage) getApplyState() *mraft.RaftApplyState {
	return &ps.applyState
}

func (ps *peerStorage) getAppliedIndex() uint64 {
	return ps.getApplyState().AppliedIndex
}

func (ps *peerStorage) getTruncatedIndex() uint64 {
	return ps.getApplyState().TruncatedState.Index
}

func (ps *peerStorage) getTruncatedTerm() uint64 {
	return ps.getApplyState().TruncatedState.Term
}

func (ps *peerStorage) getAppliedIndexTerm() uint64 {
	return atomic.LoadUint64(&ps.appliedIndexTerm)
}

func (ps *peerStorage) setAppliedIndexTerm(appliedIndexTerm uint64) {
	atomic.StoreUint64(&ps.appliedIndexTerm, appliedIndexTerm)
}

func (ps *peerStorage) validateSnap(snap *raftpb.Snapshot) bool {
	idx := snap.Metadata.Index

	if idx < ps.getTruncatedIndex() {
		// stale snapshot, should generate again.
		log.Infof("raftstore[cell-%d]: snapshot is stale, generate again, snapIndex=<%d> currIndex=<%d>",
			ps.cell.ID,
			idx,
			ps.getTruncatedIndex())
		return false
	}

	snapData := &mraft.RaftSnapshotData{}
	err := snapData.Unmarshal(snap.Data)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: decode snapshot fail, errors:\n %+v",
			ps.cell.ID,
			err)
		return false
	}

	snapEpoch := snapData.Cell.Epoch
	lastEpoch := ps.cell.Epoch

	if snapEpoch.ConfVer < lastEpoch.ConfVer {
		log.Infof("raftstore[cell-%d]: snapshot epoch stale, generate again. snap=<%s> curr=<%s>",
			ps.cell.ID,
			snapEpoch.String(),
			lastEpoch.String())
		return false
	}

	return true
}

func (ps *peerStorage) isInitialized() bool {
	return len(ps.cell.Peers) != 0
}

func (ps *peerStorage) isApplyingSnapshot() bool {
	return ps.applySnapJob != nil && ps.applySnapJob.IsNotComplete()
}

func (ps *peerStorage) getCell() metapb.Cell {
	return ps.cell
}

func (ps *peerStorage) checkRange(low, high uint64) error {
	if low > high {
		return fmt.Errorf("raftstore[cell-%d]: low is greater that high, low=<%d> high=<%d>",
			ps.cell.ID,
			low,
			high)
	} else if low <= ps.getTruncatedIndex() {
		return raft.ErrCompacted
	} else {
		i, err := ps.LastIndex()
		if err != nil {
			return err
		}

		if high > i+1 {
			return fmt.Errorf("raftstore[cell-%d]: entries' high is out of bound lastindex, hight=<%d> lastindex=<%d>",
				ps.cell.ID,
				high,
				i)
		}
	}

	return nil
}

func (ps *peerStorage) loadLogEntry(index uint64) (*raftpb.Entry, error) {
	key := getRaftLogKey(ps.cell.ID, index)
	v, err := ps.store.engine.Get(key)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: load entry failure, index=<%d> errors:\n %+v",
			ps.cell.ID,
			index,
			err)
		return nil, err
	} else if v == nil {
		log.Errorf("raftstore[cell-%d]: entry not found, index=<%d>",
			ps.cell.ID,
			index)
		return nil, fmt.Errorf("log entry at <%d> not found", index)
	}

	return ps.unmarshal(v, index)
}

func (ps *peerStorage) loadCellLocalState(job *util.Job) (*mraft.CellLocalState, error) {
	if nil != job &&
		job.IsCancelling() {
		return nil, util.ErrJobCancelled
	}

	key := getCellStateKey(ps.cell.ID)
	v, err := ps.store.engine.Get(key)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: load raft state failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return nil, err
	} else if v == nil {
		return nil, errors.New("cell state not found")
	}

	stat := &mraft.CellLocalState{}
	err = stat.Unmarshal(v)

	return stat, err
}

func (ps *peerStorage) loadSnapshot(job *util.Job) (*raftpb.Snapshot, error) {
	if nil != job &&
		job.IsCancelling() {
		return nil, util.ErrJobCancelled
	}

	snapshot, err := ps.snapshortter.Load()
	if err != nil {
		log.Errorf("raftstore[cell-%d]: load snapshot failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return nil, err
	}

	return snapshot, nil
}

func (ps *peerStorage) loadApplyState() (*mraft.RaftApplyState, error) {
	key := getApplyStateKey(ps.cell.ID)
	v, err := ps.store.engine.Get(key)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: load apply state failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return nil, err
	}

	if nil == v {
		return nil, errors.New("cell apply state not found")
	}

	applyState := &mraft.RaftApplyState{}
	err = applyState.Unmarshal(v)
	return applyState, err
}

func (ps *peerStorage) unmarshal(v []byte, expectIndex uint64) (*raftpb.Entry, error) {
	e := &raftpb.Entry{}
	if err := e.Unmarshal(v); err != nil {
		log.Errorf("raftstore[cell-%d]: unmarshal entry failure, index=<%d> errors:\n %+v",
			ps.cell.ID,
			expectIndex,
			err)
		return nil, err
	}

	if e.Index != expectIndex {
		log.Fatalf("raftstore[cell-%d]: raft log index not match, logIndex=<%d> expect=<%d>",
			ps.cell.ID,
			e.Index,
			expectIndex)
	}

	return e, nil
}

func (ps *peerStorage) clearMeta() error {
	metaCount := 0
	raftCount := 0

	// meta must in the range [cellID, cellID + 1)
	metaStart := getCellMetaPrefix(ps.cell.ID)
	metaEnd := getCellMetaPrefix(ps.cell.ID + 1)

	err := ps.store.engine.Scan(metaStart, metaEnd, func(key []byte) (bool, error) {
		err := ps.store.engine.Delete(key)
		if err != nil {
			return false, errors.Wrapf(err, "")
		}

		metaCount++
		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "")
	}

	raftStart := getCellRaftPrefix(ps.cell.ID)
	raftEnd := getCellRaftPrefix(ps.cell.ID + 1)

	err = ps.store.engine.Scan(raftStart, raftEnd, func(key []byte) (bool, error) {
		err := ps.store.engine.Delete(key)
		if err != nil {
			return false, errors.Wrapf(err, "")
		}

		raftCount++
		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "")
	}

	log.Infof("raftstore[cell-%d]: clear peer meta keys and raft keys, meta key count=<%d>, raft key count=<%d>",
		ps.cell.ID,
		metaCount,
		raftCount)

	return nil
}

// Delete all data that is not covered by `newCell`.
func (ps *peerStorage) clearExtraData(newCell metapb.Cell) error {
	oldStartKey := encStartKey(ps.cell)
	oldEndKey := encEndKey(ps.cell)

	newStartKey := encStartKey(newCell)
	newEndKey := encEndKey(newCell)

	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		err := ps.startDestroyDataJob(newCell.ID, oldStartKey, newStartKey)

		if err != nil {
			return err
		}
	}

	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		err := ps.startDestroyDataJob(newCell.ID, newEndKey, oldEndKey)

		if err != nil {
			return err
		}
	}

	return nil
}

func (ps *peerStorage) updatePeerState(cell metapb.Cell, state mraft.PeerState) error {
	cellState := mraft.CellLocalState{}
	cellState.State = state
	cellState.Cell = cell

	data, _ := cellState.Marshal()
	return ps.store.engine.Set(getCellStateKey(cell.ID), data)
}

func (ps *peerStorage) deleteAllInRange(start, end []byte, job *util.Job) error {
	if job != nil &&
		job.IsCancelling() {
		return util.ErrJobCancelled
	}

	// TODO: impl
	return nil
}
