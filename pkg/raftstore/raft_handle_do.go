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
	"sync/atomic"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/etcd/raft"
	"github.com/deepfabric/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type readyContext struct {
	raftState  mraft.RaftLocalState
	applyState mraft.RaftApplyState
	lastTerm   uint64
	snap       *mraft.SnapshotMessage
	wb         storage.WriteBatch
}

func (ctx *readyContext) reset() {
	ctx.raftState = emptyRaftState
	ctx.applyState = emptyApplyState
	ctx.lastTerm = 0
	ctx.snap = nil
	ctx.wb = nil
}

type splitCheckResult struct {
	cellID   uint64
	epoch    metapb.CellEpoch
	splitKey []byte
}

type applySnapResult struct {
	prevCell metapb.Cell
	cell     metapb.Cell
}

type readIndexQueue struct {
	cellID   uint64
	reads    []*cmd
	readyCnt int32
}

func (q *readIndexQueue) push(c *cmd) {
	q.reads = append(q.reads, c)
}

func (q *readIndexQueue) pop() *cmd {
	if len(q.reads) == 0 {
		return nil
	}

	value := q.reads[0]
	q.reads[0] = nil
	q.reads = q.reads[1:]

	return value
}

func (q *readIndexQueue) incrReadyCnt() int32 {
	q.readyCnt++
	return q.readyCnt
}

func (q *readIndexQueue) decrReadyCnt() int32 {
	q.readyCnt--
	return q.readyCnt
}

func (q *readIndexQueue) resetReadyCnt() {
	q.readyCnt = 0
}

func (q *readIndexQueue) getReadyCnt() int32 {
	return atomic.LoadInt32(&q.readyCnt)
}

func (q *readIndexQueue) size() int {
	return len(q.reads)
}

// ====================== raft ready handle methods
func (ps *peerStorage) doAppendSnapshot(ctx *readyContext, snap raftpb.Snapshot) error {
	log.Infof("raftstore[cell-%d]: begin to apply snapshot", ps.getCell().ID)

	if ctx.snap.Header.Cell.ID != ps.getCell().ID {
		return fmt.Errorf("raftstore[cell-%d]: cell not match, snapCell=<%d> currCell=<%d>",
			ps.getCell().ID,
			ctx.snap.Header.Cell.ID,
			ps.getCell().ID)
	}

	if ps.isInitialized() {
		err := ps.store.clearMeta(ps.cell.ID, ctx.wb)
		if err != nil {
			log.Errorf("raftstore[cell-%d]: clear meta failed, errors:\n %+v",
				ps.getCell().ID,
				err)
			return err
		}
	}

	err := ps.updatePeerState(ctx.snap.Header.Cell, mraft.Applying, ctx.wb)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: write peer state failed, errors:\n %+v",
			ps.getCell().ID,
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
		ps.getCell().ID,
		ctx.applyState.String())

	return nil
}

// doAppendEntries the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *peerStorage) doAppendEntries(ctx *readyContext, entries []raftpb.Entry) error {
	c := len(entries)

	if c == 0 {
		return nil
	}

	log.Debugf("raftstore[cell-%d]: append entries, count=<%d>",
		ps.getCell().ID,
		c)

	prevLastIndex := ctx.raftState.LastIndex
	lastIndex := entries[c-1].Index
	lastTerm := entries[c-1].Term

	for _, e := range entries {
		d := util.MustMarshal(&e)
		err := ctx.wb.Set(getRaftLogKey(ps.getCell().ID, e.Index), d)
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: append entry failure, entry=<%s> errors:\n %+v",
				ps.getCell().ID,
				e.String(),
				err)
			return err
		}
	}

	// Delete any previously appended log entries which never committed.
	for index := lastIndex + 1; index < prevLastIndex+1; index++ {
		err := ctx.wb.Delete(getRaftLogKey(ps.getCell().ID, index))
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: delete any previously appended log entries failure, index=<%d> errors:\n %+v",
				ps.getCell().ID,
				index,
				err)
			return err
		}
	}

	ctx.raftState.LastIndex = lastIndex
	ctx.lastTerm = lastTerm

	return nil
}

func (pr *PeerReplicate) doSaveRaftState(ctx *readyContext) error {
	data, _ := ctx.raftState.Marshal()
	err := ctx.wb.Set(getRaftStateKey(pr.ps.getCell().ID), data)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: save temp raft state failure, errors:\n %+v",
			pr.ps.getCell().ID,
			err)
	}

	return err
}

func (pr *PeerReplicate) doSaveApplyState(ctx *readyContext) error {
	err := ctx.wb.Set(getApplyStateKey(pr.ps.getCell().ID), util.MustMarshal(&ctx.applyState))
	if err != nil {
		log.Errorf("raftstore[cell-%d]: save temp apply state failure, errors:\n %+v",
			pr.ps.getCell().ID,
			err)
	}

	return err
}

func (pr *PeerReplicate) doApplySnap(ctx *readyContext, rd *raft.Ready) *applySnapResult {
	pr.ps.raftState = ctx.raftState
	pr.ps.setApplyState(&ctx.applyState)
	pr.ps.lastTerm = ctx.lastTerm

	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.snap == nil {
		return nil
	}

	// cleanup data before apply snap job
	if pr.ps.isInitialized() {
		err := pr.ps.clearExtraData(pr.ps.getCell())
		if err != nil {
			// No need panic here, when applying snapshot, the deletion will be tried
			// again. But if the cell range changes, like [a, c) -> [a, b) and [b, c),
			// [b, c) will be kept in rocksdb until a covered snapshot is applied or
			// store is restarted.
			log.Errorf("raftstore[cell-%d]: cleanup data failed, may leave some dirty data, errors:\n %+v",
				pr.cellID,
				err)
			return nil
		}
	}

	pr.startApplyingSnapJob()

	// remove pending snapshots for sending
	rms := removedPeers(ctx.snap.Header.Cell, pr.ps.getCell())
	for _, p := range rms {
		pr.store.trans.forceRemoveSendingSnapshot(p)
	}

	prevCell := pr.ps.getCell()
	pr.ps.setCell(ctx.snap.Header.Cell)

	return &applySnapResult{
		prevCell: prevCell,
		cell:     pr.ps.getCell(),
	}
}

func (pr *PeerReplicate) applyCommittedEntries(rd *raft.Ready) {
	if pr.ps.isApplyingSnap() {
		pr.ps.lastReadyIndex = pr.ps.getTruncatedIndex()
	} else {
		for _, entry := range rd.CommittedEntries {
			pr.raftLogSizeHint += uint64(len(entry.Data))
		}

		if len(rd.CommittedEntries) > 0 {
			pr.ps.lastReadyIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index

			err := pr.startApplyCommittedEntriesJob(pr.cellID, pr.getCurrentTerm(), rd.CommittedEntries)
			if err != nil {
				log.Fatalf("raftstore[cell-%d]: add apply committed entries job failed, errors:\n %+v",
					pr.cellID,
					err)
			}
		}
	}
}

func (pr *PeerReplicate) doPropose(c *cmd, isConfChange bool) error {
	delegate := pr.store.delegates.get(pr.cellID)
	if delegate == nil {
		c.respCellNotFound(pr.cellID)
		return nil
	}

	if delegate.cell.ID != pr.cellID {
		log.Fatal("bug: delegate id not match")
	}

	if isConfChange {
		changeC := delegate.getPendingChangePeerCMD()
		if nil != changeC && changeC.req != nil && changeC.req.Header != nil {
			delegate.notifyStaleCMD(changeC)
		}
		delegate.setPendingChangePeerCMD(c)
	} else {
		delegate.appendPendingCmd(c)
	}

	return nil
}

func (pr *PeerReplicate) doSplitCheck(epoch metapb.CellEpoch, startKey, endKey []byte) error {
	var size uint64
	var splitKey []byte

	size, splitKey, err := pr.store.getDataEngine().GetTargetSizeKey(startKey, endKey, globalCfg.CellCapacity)

	if err != nil {
		log.Errorf("raftstore-split[cell-%d]: failed to scan split key, errors:\n %+v",
			pr.cellID,
			err)
		return err
	}

	if len(splitKey) == 0 {
		log.Debugf("raftstore-split[cell-%d]: no need to split, size=<%d> capacity=<%d> start=<%v> end=<%v>",
			pr.cellID,
			size,
			globalCfg.CellCapacity,
			startKey,
			endKey)
		pr.sizeDiffHint = size
		return nil
	}

	log.Infof("raftstore-split[cell-%d]: try to split, size=<%d> splitKey=<%d>",
		pr.cellID,
		size,
		splitKey)

	pr.store.notify(&splitCheckResult{
		cellID:   pr.cellID,
		splitKey: splitKey,
		epoch:    epoch,
	})

	return nil
}

func (pr *PeerReplicate) doAskSplit(cell metapb.Cell, peer metapb.Peer, splitKey []byte) error {
	req := &pdpb.AskSplitReq{
		Cell: cell,
	}

	rsp, err := pr.store.pdClient.AskSplit(context.TODO(), req)
	if err != nil {
		log.Errorf("raftstore-split[cell-%d]: ask split to pd failed, error:\n %+v",
			pr.cellID,
			err)
		return err
	}

	splitReq := new(raftcmdpb.SplitRequest)
	splitReq.NewCellID = rsp.NewCellID
	splitReq.SplitKey = splitKey
	splitReq.NewPeerIDs = rsp.NewPeerIDs

	adminReq := new(raftcmdpb.AdminRequest)
	adminReq.Type = raftcmdpb.Split
	adminReq.Body = util.MustMarshal(splitReq)

	pr.onAdminRequest(adminReq)
	return nil
}

func (pr *PeerReplicate) doPostApply(result *asyncApplyResult) {
	if pr.ps.isApplyingSnap() {
		log.Fatalf("raftstore[cell-%d]: should not applying snapshot, when do post apply.",
			pr.cellID)
	}

	pr.ps.setApplyState(&result.applyState)
	pr.ps.setAppliedIndexTerm(result.appliedIndexTerm)
	pr.rn.AdvanceApply(result.applyState.AppliedIndex)

	log.Debugf("raftstore[cell-%d]: async apply committied entries finished, applied=<%d>",
		pr.cellID,
		result.applyState.AppliedIndex)

	pr.metrics.admin.incBy(result.metrics.admin)

	pr.writtenBytes += uint64(result.metrics.writtenBytes)
	pr.writtenKeys += result.metrics.writtenKeys

	if result.hasSplitExecResult() {
		pr.deleteKeysHint = result.metrics.deleteKeysHint
		pr.sizeDiffHint = result.metrics.sizeDiffHint
	} else {
		pr.deleteKeysHint += result.metrics.deleteKeysHint
		pr.sizeDiffHint += result.metrics.sizeDiffHint
	}

	readyCnt := int(pr.pendingReads.getReadyCnt())
	if readyCnt > 0 && pr.readyToHandleRead() {
		for index := 0; index < readyCnt; index++ {
			pr.doExecReadCmd(pr.pendingReads.pop())
		}

		pr.pendingReads.resetReadyCnt()
	}
}

func (s *Store) doPostApplyResult(result *asyncApplyResult) {
	switch result.result.adminType {
	case raftcmdpb.ChangePeer:
		s.doApplyConfChange(result.cellID, result.result.changePeer)
	case raftcmdpb.Split:
		s.doApplySplit(result.cellID, result.result.splitResult)
	case raftcmdpb.RaftLogGC:
		s.doApplyRaftLogGC(result.cellID, result.result.raftGCResult)
	}
}

func (s *Store) doApplyConfChange(cellID uint64, cp *changePeer) {
	pr := s.replicatesMap.get(cellID)
	if nil == pr {
		log.Fatalf("raftstore-apply[cell-%d]: missing cell",
			cellID)
	}

	pr.rn.ApplyConfChange(cp.confChange)
	if cp.confChange.NodeID == pd.ZeroID {
		// Apply failed, skip.
		return
	}

	pr.ps.setCell(cp.cell)

	if pr.isLeader() {
		// Notify pd immediately.
		log.Infof("raftstore-apply[cell-%d]: notify pd with change peer, cell=<%+v>",
			cellID,
			cp.cell)
		pr.doHeartbeat()
	}

	switch cp.confChange.Type {
	case raftpb.ConfChangeAddNode:
		// Add this peer to cache.
		pr.peerHeartbeatsMap.put(cp.peer.ID, time.Now())
		s.peerCache.put(cp.peer.ID, cp.peer)
	case raftpb.ConfChangeRemoveNode:
		// Remove this peer from cache.
		pr.peerHeartbeatsMap.delete(cp.peer.ID)
		s.peerCache.delete(cp.peer.ID)

		// We only care remove itself now.
		if cp.peer.StoreID == pr.store.GetID() {
			if cp.peer.ID == pr.peer.ID {
				s.destroyPeer(cellID, cp.peer, false)
			} else {
				log.Fatalf("raftstore-apply[cell-%d]: trying to remove unknown peer, peer=<%+v>",
					cellID,
					cp.peer)
			}
		}
	}
}

func (s *Store) doApplySplit(cellID uint64, result *splitResult) {
	pr := s.replicatesMap.get(cellID)
	if nil == pr {
		log.Fatalf("raftstore-apply[cell-%d]: missing cell",
			cellID)
	}

	left := result.left
	right := result.right

	pr.ps.setCell(left)

	// add new cell peers to cache
	for _, p := range right.Peers {
		s.peerCache.put(p.ID, *p)
	}

	newCellID := right.ID
	newPR := s.replicatesMap.get(newCellID)
	if nil != newPR {
		for _, p := range right.Peers {
			s.peerCache.put(p.ID, *p)
		}

		// If the store received a raft msg with the new region raft group
		// before splitting, it will creates a uninitialized peer.
		// We can remove this uninitialized peer directly.
		if newPR.ps.isInitialized() {
			log.Fatalf("raftstore-apply[cell-%d]: duplicated cell for split, newCellID=<%d>",
				cellID,
				newCellID)
		}
	}

	newPR, err := createPeerReplicate(s, &right)
	if err != nil {
		// peer information is already written into db, can't recover.
		// there is probably a bug.
		log.Fatalf("raftstore-apply[cell-%d]: create new split cell failed, newCell=<%d> errors:\n %+v",
			cellID,
			right,
			err)
	}

	// If this peer is the leader of the cell before split, it's intuitional for
	// it to become the leader of new split cell.
	// The ticks are accelerated here, so that the peer for the new split cell
	// comes to campaign earlier than the other follower peers. And then it's more
	// likely for this peer to become the leader of the new split cell.
	// If the other follower peers applies logs too slowly, they may fail to vote the
	// `MsgRequestVote` from this peer on its campaign.
	// In this worst case scenario, the new split raft group will not be available
	// since there is no leader established during one election timeout after the split.
	if pr.isLeader() && len(right.Peers) > 1 {
		succ, err := newPR.maybeCampaign()
		if err != nil {
			log.Fatalf("raftstore-apply[cell-%d]: new split cell campaign failed, newCell=<%d> errors:\n %+v",
				cellID,
				right,
				err)
		}

		if succ {
			newPR.onRaftTick(nil)
		}
	}

	if pr.isLeader() {
		log.Infof("raftstore-apply[cell-%d]: notify pd with split, left=<%+v> right=<%+v>, state=<%s>, apply=<%s>",
			cellID,
			left,
			right,
			pr.ps.raftState.String(),
			pr.ps.applyState.String())

		pr.doHeartbeat()
		newPR.doHeartbeat()

		err := s.startReportSpltJob(left, right)
		if err != nil {
			log.Errorf("raftstore-apply[cell-%d]: add report split job failed, errors:\n %+v",
				cellID,
				err)
		}
	}

	s.keyRanges.Update(left)
	s.keyRanges.Update(right)

	newPR.sizeDiffHint = globalCfg.ThresholdSplitCheckBytes
	newPR.startRegistrationJob()
	s.replicatesMap.put(newPR.cellID, newPR)

	if err = s.notifySplitCellIndex(left.GetID(), right.GetID()); err != nil {
		log.Errorf("raftstore-apply[cell-%d]: doExecSplitIndex failed\n%+v",
			left.GetID(), err)
	}

	log.Infof("raftstore-apply[cell-%d]: new cell added, left=<%+v> right=<%+v>",
		cellID,
		left,
		right)
}

func (s *Store) doApplyRaftLogGC(cellID uint64, result *raftGCResult) {
	pr := s.replicatesMap.get(cellID)
	if pr != nil {
		total := pr.ps.lastReadyIndex - result.firstIndex
		remain := pr.ps.lastReadyIndex - result.state.Index - 1
		pr.raftLogSizeHint = pr.raftLogSizeHint * remain / total

		startIndex := pr.ps.lastCompactIndex
		endIndex := result.state.Index + 1
		pr.ps.lastCompactIndex = endIndex

		log.Debugf("raftstore-apply[cell-%d]: start to compact raft log, start=<%d> end=<%d>",
			cellID,
			startIndex,
			endIndex)
		err := pr.startRaftLogGCJob(cellID, startIndex, endIndex)
		if err != nil {
			log.Errorf("raftstore-compact[cell-%d]: add raft gc job failed, errors:\n %+v",
				cellID,
				err)
		}
	}
}

func (pr *PeerReplicate) doApplyReads(rd *raft.Ready) {
	if pr.readyToHandleRead() {
		for _, state := range rd.ReadStates {
			c := pr.pendingReads.pop()
			if bytes.Compare(state.RequestCtx, c.getUUID()) != 0 {
				log.Fatalf("raftstore[cell-%d]: apply read failed, uuid not match",
					pr.cellID)
			}

			pr.doExecReadCmd(c)
		}
	} else {
		for _ = range rd.ReadStates {
			pr.pendingReads.incrReadyCnt()
		}
	}

	// Note that only after handle read_states can we identify what requests are
	// actually stale.
	if rd.SoftState != nil {
		if rd.SoftState.RaftState != raft.StateLeader {
			n := int(pr.pendingReads.size())
			if n > 0 {
				// all uncommitted reads will be dropped silently in raft.
				for index := 0; index < n; index++ {
					c := pr.pendingReads.pop()
					resp := errorStaleCMDResp(c.getUUID(), pr.getCurrentTerm())

					log.Debugf("raftstore[cell-%d]: resp stale, cmd=<%+v>",
						pr.cellID,
						c)
					c.resp(resp)
				}
			}

			pr.pendingReads.resetReadyCnt()

			// we are not leader now, so all writes in the batch is actually stale
			for i := 0; i < pr.batch.size(); i++ {
				c := pr.batch.pop()
				resp := errorStaleCMDResp(c.getUUID(), pr.getCurrentTerm())
				log.Debugf("raftstore[cell-%d]: resp stale, cmd=<%+v>",
					pr.cellID,
					c)
				c.resp(resp)
			}
			pr.resetBatch()
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
				ps.getCell().ID,
				hardState)
		}

		return hardState, confState, nil
	}

	for _, p := range ps.getCell().Peers {
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

	startKey := getRaftLogKey(ps.getCell().ID, low)

	if low+1 == high {
		// If election happens in inactive cells, they will just try
		// to fetch one empty log.
		v, err := ps.store.getMetaEngine().Get(startKey)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		if len(v) == 0 {
			return nil, raft.ErrUnavailable
		}

		e, err := ps.unmarshal(v, low)
		if err != nil {
			releaseEntry(e)
			return nil, err
		}

		ents = append(ents, *e)
		releaseEntry(e)
		return ents, nil
	}

	endKey := getRaftLogKey(ps.getCell().ID, high)
	err = ps.store.getMetaEngine().Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		e := acquireEntry()
		util.MustUnmarshal(e, value)

		// May meet gap or has been compacted.
		if e.Index != nextIndex {
			return false, nil
		}

		nextIndex++
		totalSize += uint64(len(value))

		exceededMaxSize = totalSize > maxSize
		if !exceededMaxSize || len(ents) == 0 {
			ents = append(ents, *e)
			releaseEntry(e)
		}

		return !exceededMaxSize, nil
	}, false)

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

	key := getRaftLogKey(ps.getCell().ID, idx)
	v, err := ps.store.getMetaEngine().Get(key)
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, raft.ErrUnavailable
	}

	e, err := ps.unmarshal(v, idx)
	if err != nil {
		releaseEntry(e)
		return 0, err
	}

	t := e.Term
	releaseEntry(e)
	return t, nil
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
		result := ps.genSnapJob.GetResult()
		// snapshot failure, we will continue try do snapshot
		if nil == result {
			log.Warnf("raftstore[cell-%d]: snapshot generating failed, triedCnt=<%d>",
				ps.getCell().ID,
				ps.snapTriedCnt)
			ps.snapTriedCnt++
		} else {
			snap := result.(raftpb.Snapshot)
			if ps.validateSnap(&snap) {
				ps.resetGenSnapJob()
				return snap, nil
			}
		}
	}

	if ps.snapTriedCnt >= maxSnapTryCnt {
		cnt := ps.snapTriedCnt
		ps.resetGenSnapJob()
		return raftpb.Snapshot{}, fmt.Errorf("raftstore[cell-%d]: failed to get snapshot after %d times",
			ps.getCell().ID,
			cnt)
	}

	log.Infof("raftstore[cell-%d]: start snapshot, epoch=<%+v>",
		ps.getCell().ID,
		ps.getCell().Epoch)
	ps.snapTriedCnt++

	err := ps.store.addSnapJob(ps.doGenerateSnapshotJob, ps.setGenSnapJob)
	if err != nil {
		log.Fatalf("raftstore[cell-%d]: add generate job failed, errors:\n %+v",
			ps.getCell().ID,
			err)
	}

	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *peerStorage) setGenSnapJob(job *util.Job) {
	ps.genSnapJob = job
}

func (ps *peerStorage) setApplySnapJob(job *util.Job) {
	ps.applySnapJob = job
}

func (s *Store) notifySplitCellIndex(leftCellID uint64, rightCellID uint64) (err error) {
	listEng := s.getListEngine()
	idxReqQueueKey := getIdxReqQueueKey()
	idxReq := &pdpb.IndexRequest{
		IdxSplit: &pdpb.IndexSplitRequest{
			LeftCellID:  leftCellID,
			RightCellID: rightCellID,
		},
	}
	var idxReqB []byte
	idxReqB, err = idxReq.Marshal()
	if err != nil {
		return
	}
	_, err = listEng.RPush(idxReqQueueKey, idxReqB)
	return
}

func (s *Store) notifyDestroyCellIndex(cell *metapb.Cell) (err error) {
	listEng := s.getListEngine()
	idxReqQueueKey := getIdxReqQueueKey()
	idxReq := &pdpb.IndexRequest{
		IdxDestroy: &pdpb.IndexDestroyCellRequest{
			CellID: cell.GetID(),
		},
	}
	var idxReqB []byte
	idxReqB, err = idxReq.Marshal()
	if err != nil {
		return
	}
	_, err = listEng.RPush(idxReqQueueKey, idxReqB)
	return
}

func (s *Store) notifyRebuildCellIndex(cell *metapb.Cell) (err error) {
	listEng := s.getListEngine()
	idxReqQueueKey := getIdxReqQueueKey()
	idxReq := &pdpb.IndexRequest{
		IdxRebuild: &pdpb.IndexRebuildCellRequest{
			CellID: cell.GetID(),
		},
	}
	var idxReqB []byte
	idxReqB, err = idxReq.Marshal()
	if err != nil {
		return
	}
	_, err = listEng.RPush(idxReqQueueKey, idxReqB)
	return
}
