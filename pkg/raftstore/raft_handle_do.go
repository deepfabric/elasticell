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
	"sync/atomic"

	"bytes"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

type tempRaftContext struct {
	raftState  mraft.RaftLocalState
	applyState mraft.RaftApplyState
	lastTerm   uint64
	snapCell   *metapb.Cell
}

type applySnapResult struct {
	prevCell metapb.Cell
	cell     metapb.Cell
}

type readIndexRequest struct {
	uuid []byte
	cmds []*cmd
}

type readIndexQueue struct {
	reads    *queue.RingBuffer
	readyCnt int32
}

func (q *readIndexQueue) mustGet(cellID uint64) *readIndexRequest {
	v, err := q.reads.Get()
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: handle read index failed, errors:\n %+v",
			cellID,
			err)
	}

	return v.(*readIndexRequest)
}

func (q *readIndexQueue) incrReadyCnt() int32 {
	return atomic.AddInt32(&q.readyCnt, 1)
}

func (q *readIndexQueue) decrReadyCnt() int32 {
	return atomic.AddInt32(&q.readyCnt, -1)
}

func (q *readIndexQueue) resetReadyCnt() {
	atomic.StoreInt32(&q.readyCnt, 0)
}

func (q *readIndexQueue) getReadyCnt() int32 {
	return atomic.LoadInt32(&q.readyCnt)
}

type cmd struct {
	req *raftcmdpb.RaftCMDRequest
	cb  func(*raftcmdpb.RaftCMDResponse)
}

// ====================== raft ready handle methods
func (ps *peerStorage) doAppendSnapshot(ctx *tempRaftContext, snap raftpb.Snapshot) error {
	log.Infof("raftstore[cell-%d]: begin to apply snapshot", ps.cell.ID)

	snapData := &mraft.RaftSnapshotData{}
	util.MustUnmarshal(snapData, snap.Data)

	if snapData.Cell.ID != ps.cell.ID {
		return fmt.Errorf("raftstore[cell-%d]: cell not match, snapCell=<%d> currCell=<%d>",
			ps.cell.ID,
			snapData.Cell.ID,
			ps.cell.ID)
	}

	if ps.isInitialized() {
		err := ps.clearMeta()
		if err != nil {
			log.Errorf("raftstore[cell-%d]: clear meta failed, errors:\n %+v",
				ps.cell.ID,
				err)
			return err
		}
	}

	err := ps.updatePeerState(ps.cell, mraft.Applying)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: write peer state failed, errors:\n %+v",
			ps.cell.ID,
			err)
		return err
	}

	lastIndex := snap.Metadata.Index
	lastTerm := snap.Metadata.Term

	ctx.raftState.LastIndex = lastIndex
	ctx.applyState.AppliedIndex = lastIndex
	ctx.lastTerm = lastTerm

	// The snapshot only contains log which index > applied index, so
	// here the truncate state's (index, term) is in snapshot metadata.
	ctx.applyState.TruncatedState.Index = lastIndex
	ctx.applyState.TruncatedState.Term = lastTerm

	log.Infof("raftstore[cell-%d]: apply snapshot ok, state=<%s>",
		ps.cell.ID,
		ctx.applyState.String())

	c := snapData.Cell
	ctx.snapCell = &c

	return nil
}

// doAppendEntries the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *peerStorage) doAppendEntries(ctx *tempRaftContext, entries []raftpb.Entry) error {
	c := len(entries)

	log.Debugf("raftstore[cell-%d]: append entries, count=<%d>",
		ps.cell.ID,
		c)

	if c == 0 {
		return nil
	}

	prevLastIndex := ctx.raftState.LastIndex
	lastIndex := entries[c-1].Index
	lastTerm := entries[c-1].Term

	for _, e := range entries {
		d := util.MustMarshal(&e)
		err := ps.store.engine.Set(getRaftLogKey(ps.cell.ID, e.Index), d)
		if err != nil {
			log.Errorf("raftstore[cell-%d]: append entry failure, entry=<%s> errors:\n %+v",
				ps.cell.ID,
				e.String(),
				err)
			return err
		}
	}

	// Delete any previously appended log entries which never committed.
	for index := lastIndex + 1; index < prevLastIndex+1; index++ {
		err := ps.store.engine.Delete(getRaftLogKey(ps.cell.ID, index))
		if err != nil {
			log.Errorf("raftstore[cell-%d]: delete any previously appended log entries failure, index=<%d> errors:\n %+v",
				ps.cell.ID,
				index,
				err)
			return err
		}
	}

	ctx.raftState.LastIndex = lastIndex
	ctx.lastTerm = lastTerm

	return nil
}

func (pr *PeerReplicate) doSaveRaftState(ctx *tempRaftContext) error {
	data, _ := ctx.raftState.Marshal()
	err := pr.store.engine.Set(getRaftStateKey(pr.ps.cell.ID), data)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: save temp raft state failure, errors:\n %+v",
			pr.ps.cell.ID,
			err)
	}

	return err
}

func (pr *PeerReplicate) doSaveApplyState(ctx *tempRaftContext) error {
	err := pr.store.engine.Set(getApplyStateKey(pr.ps.cell.ID), util.MustMarshal(&ctx.applyState))
	if err != nil {
		log.Errorf("raftstore[cell-%d]: save temp apply state failure, errors:\n %+v",
			pr.ps.cell.ID,
			err)
	}

	return err
}

func (pr *PeerReplicate) doApplySnap(ctx *tempRaftContext) *applySnapResult {
	pr.ps.raftState = ctx.raftState
	pr.ps.setApplyState(&ctx.applyState)
	pr.ps.lastTerm = ctx.lastTerm

	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.snapCell == nil {
		return nil
	}

	// cleanup data before apply snap job
	if pr.ps.isInitialized() {
		// TODO: why??
		err := pr.ps.clearExtraData(pr.ps.cell)
		if err != nil {
			// No need panic here, when applying snapshot, the deletion will be tried
			// again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
			// [b, c) will be kept in rocksdb until a covered snapshot is applied or
			// store is restarted.
			log.Errorf("raftstore[cell-%d]: cleanup data failed, may leave some dirty data, errors:\n %+v",
				pr.cellID,
				err)
			return nil
		}
	}

	pr.startApplyingSnapJob()

	prevCell := pr.ps.cell
	pr.ps.cell = *ctx.snapCell

	return &applySnapResult{
		prevCell: prevCell,
		cell:     pr.ps.cell,
	}
}

func (pr *PeerReplicate) applyCommittedEntries(rd *raft.Ready) bool {
	if !pr.ps.isApplyingSnap() {
		// TODO: update lease??

		if len(rd.CommittedEntries) > 0 {
			err := pr.startApplyCommittedEntriesJob(pr.ps.cell.ID, pr.getCurrentTerm(), rd.CommittedEntries)
			if err != nil {
				log.Fatalf("raftstore[cell-%d]: add apply committed entries job failed, errors:\n %+v",
					pr.ps.cell.ID,
					err)
			}

			return true
		}
	}

	return false
}

func (pr *PeerReplicate) doPostApply(result *asyncApplyResult) {
	if pr.ps.isApplyingSnap() {
		log.Fatalf("raftstore[cell-%d]: should not applying snapshot, when do post apply.",
			pr.cellID)
	}

	log.Debugf("raftstore[cell-%d]: async apply committied entries finished", pr.cellID)

	pr.ps.setApplyState(&result.applyState)
	pr.ps.setAppliedIndexTerm(result.appliedIndexTerm)

	readyCnt := int(pr.pendingReads.getReadyCnt())
	if readyCnt > 0 && pr.readyToHandleRead() {
		for index := 0; index < readyCnt; index++ {
			pr.execReadRequest(pr.pendingReads.mustGet(pr.cellID))
		}

		pr.pendingReads.resetReadyCnt()
	}
}

func (pr *PeerReplicate) doApplyReads(rd *raft.Ready) {
	if pr.readyToHandleRead() {
		for _, state := range rd.ReadStates {
			readReq := pr.pendingReads.mustGet(pr.cellID)

			if bytes.Compare(state.RequestCtx, readReq.uuid) != 0 {
				log.Fatalf("raftstore[cell-%d]: apply read failed, uuid not match",
					pr.cellID)
			}

			pr.execReadRequest(readReq)
		}
	} else {
		for _ = range rd.ReadStates {
			pr.pendingReads.incrReadyCnt()
		}
	}

	if rd.SoftState != nil {
		if rd.SoftState.RaftState != raft.StateLeader {
			n := int(pr.pendingReads.getReadyCnt())
			if n > 0 {
				for index := 0; index < n; index++ {
					req := pr.pendingReads.mustGet(pr.cellID)
					resp := errorStaleCMDResp(req.uuid, pr.getCurrentTerm())

					for _, cmd := range req.cmds {
						log.Infof("raftstore[cell-%d]: cmd is stale, skip. cmd=<%+v>",
							pr.cellID,
							cmd)
						cmd.cb(resp)
					}
				}
			}
		}
	}
}

func (pr *PeerReplicate) updateKeyRange(result *applySnapResult) {
	log.Infof("raftstore[cell-%d]: snapshot is applied, cell=<%+v>",
		pr.cellID,
		result.cell)

	if len(result.prevCell.Peers) > 0 {
		log.Infof("raftstore[cell-%d]: cell changed after apply snapshot, from=<%+v> to=<%+v>",
			pr.cellID,
			result.prevCell,
			result.cell)
		// we have already initialized the peer, so it must exist in cell_ranges.
		if !pr.store.keyRanges.Remove(result.prevCell) {
			log.Fatalf("raftstore[cell-%d]: cell not exist, cell=<%+v>",
				pr.cellID,
				result.prevCell)
		}
	}

	pr.store.keyRanges.Update(result.cell)
}

func (pr *PeerReplicate) readyToHandleRead() bool {
	// If applied_index_term isn't equal to current term, there may be some values that are not
	// applied by this leader yet but the old leader.
	return pr.ps.getAppliedIndexTerm() == pr.getCurrentTerm()
}

// ======================raft storage interface method
func (ps *peerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hardState := ps.raftState.HardState
	confState := raftpb.ConfState{}

	if hardState.Commit == 0 &&
		hardState.Term == 0 &&
		hardState.Vote == 0 {
		if ps.isInitialized() {
			log.Fatalf("raftstore[cell-%d]: cell is initialized but local state has empty hard state, hardState=<%v>",
				ps.cell.ID,
				hardState)
		}

		return hardState, confState, nil
	}

	for _, p := range ps.cell.Peers {
		confState.Nodes = append(confState.Nodes, p.ID)
	}

	return hardState, confState, nil
}

func (ps *peerStorage) Entries(low, high, maxSize uint64) ([]raftpb.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}

	var ents []raftpb.Entry
	if low == high {
		return ents, nil
	}

	var totalSize uint64
	nextIndex := low
	exceededMaxSize := false

	startKey := getRaftLogKey(ps.cell.ID, low)

	if low+1 == high {
		// If election happens in inactive cells, they will just try
		// to fetch one empty log.
		v, err := ps.store.engine.Get(startKey)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		if nil == v {
			return nil, raft.ErrUnavailable
		}

		e, err := ps.unmarshal(v, low)
		if err != nil {
			return nil, err
		}

		ents = append(ents, *e)
		return ents, nil
	}

	endKey := getRaftLogKey(ps.cell.ID, high)
	err = ps.store.engine.Scan(startKey, endKey, func(data, value []byte) (bool, error) {
		e, err := ps.unmarshal(data, nextIndex)
		if err != nil {
			return false, err
		}

		nextIndex++
		totalSize += uint64(len(data))

		exceededMaxSize = totalSize > maxSize
		if !exceededMaxSize || len(ents) == 0 {
			ents = append(ents, *e)
		}

		return !exceededMaxSize, nil
	})

	if err != nil {
		return nil, err
	}

	// If we get the correct number of entries the total size exceeds max_size, returns.
	if len(ents) == int(high-low) || exceededMaxSize {
		return ents, nil
	}

	return nil, raft.ErrUnavailable
}

func (ps *peerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.getTruncatedIndex() {
		return ps.getTruncatedTerm(), nil
	}

	err := ps.checkRange(idx, idx+1)
	if err != nil {
		return 0, err
	}

	lastIdx, err := ps.LastIndex()
	if err != nil {
		return 0, err
	}

	if ps.getTruncatedTerm() == ps.lastTerm || idx == lastIdx {
		return ps.lastTerm, nil
	}

	key := getRaftLogKey(ps.cell.ID, idx)
	v, err := ps.store.engine.Get(key)
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, raft.ErrUnavailable
	}

	e, err := ps.unmarshal(v, idx)
	if err != nil {
		return 0, err
	}

	return e.Term, nil
}

func (ps *peerStorage) LastIndex() (uint64, error) {
	return atomic.LoadUint64(&ps.raftState.LastIndex), nil
}

func (ps *peerStorage) FirstIndex() (uint64, error) {
	return ps.getTruncatedIndex() + 1, nil
}

func (ps *peerStorage) Snapshot() (raftpb.Snapshot, error) {
	if ps.isGeneratingSnap() {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	if ps.isGenSnapJobComplete() {
		result := ps.applySnapJob.GetResult()
		// snapshot failure, we will continue try do snapshot
		if nil == result {
			log.Warnf("raftstore[cell-%d]: snapshot generating failed, triedCnt=<%d>",
				ps.cell.ID,
				ps.snapTriedCnt)
			ps.snapTriedCnt++
		} else {
			snap := result.(*raftpb.Snapshot)
			ps.snapTriedCnt = 0
			if ps.validateSnap(snap) {
				ps.resetGenSnapJob()
				return *snap, nil
			}
		}
	}

	if ps.snapTriedCnt >= maxSnapTryCnt {
		cnt := ps.snapTriedCnt
		ps.resetGenSnapJob()
		return raftpb.Snapshot{}, fmt.Errorf("raftstore[cell-%d]: failed to get snapshot after %d times",
			ps.cell.ID,
			cnt)
	}

	log.Infof("raftstore[cell-%d]: start snapshot", ps.cell.ID)
	ps.snapTriedCnt++

	job, err := ps.store.addSnapJob(ps.doGenerateSnapshotJob)
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: add generate job failed, errors:\n %+v",
			ps.cell.ID,
			err)
	}
	ps.genSnapJob = job
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}
