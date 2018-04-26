package raft

import (
	"github.com/coreos/etcd/raft/raftpb"
)

// NextProposalIndex used for check operation
func (rn *RawNode) NextProposalIndex() uint64 {
	return rn.raft.raftLog.lastIndex() + 1
}

// PendingReadCount used for check operation
func (rn *RawNode) PendingReadCount() int {
	return len(rn.raft.readOnly.readIndexQueue)
}

// ReadyReadCount used for check operation
func (rn *RawNode) ReadyReadCount() int {
	return len(rn.raft.readStates)
}

// ReadySince return ready since appliedIdx
func (rn *RawNode) ReadySince(appliedIdx uint64) Ready {
	return newReadyWithSinceIdx(rn.raft, rn.prevSoftSt, rn.prevHardSt, appliedIdx)
}

// HasReadySince returns has ready since appliedIdx
func (rn *RawNode) HasReadySince(appliedIdx uint64) bool {
	raft := rn.raft

	if len(raft.msgs) != 0 || raft.raftLog.unstableEntries() != nil {
		return true
	}

	if len(raft.readStates) != 0 {
		return true
	}

	if raft.raftLog.unstable.snapshot != nil {
		return true
	}

	var hasUnappliedEntries bool
	if appliedIdx == 0 {
		hasUnappliedEntries = raft.raftLog.hasNextEnts()
	} else {
		hasUnappliedEntries = raft.raftLog.hasNextEntsSince(appliedIdx)
	}

	if hasUnappliedEntries {
		return true
	}

	ss := raft.softState()
	if ss.Lead != rn.prevSoftSt.Lead || ss.RaftState != rn.prevSoftSt.RaftState {
		return true
	}

	hs := raft.hardState()
	if !IsEmptyHardState(hs) &&
		(hs.Commit != rn.prevHardSt.Commit ||
			hs.Term != rn.prevHardSt.Term ||
			hs.Vote != rn.prevHardSt.Vote) {
		return true
	}

	return false
}

// AdvanceAppend advance append
func (rn *RawNode) AdvanceAppend(rd Ready) {
	rn.commitReadyAppend(rd)
}

// AdvanceApply advance apply
func (rn *RawNode) AdvanceApply(applied uint64) {
	rn.commitApply(applied)
}

// HasPendingSnapshot returns if there is a pending snapshot
func (rn *RawNode) HasPendingSnapshot() bool {
	return rn.raft.raftLog.unstable.snapshot != nil
}

// Term returns term of idx at raft log
func (rn *RawNode) Term(idx uint64) (uint64, error) {
	return rn.raft.raftLog.term(idx)
}

// LastIndex returns last index at raft log
func (rn *RawNode) LastIndex() uint64 {
	return rn.raft.raftLog.lastIndex()
}

func (l *raftLog) hasNextEntsSince(sinceIdx uint64) bool {
	offet := max(sinceIdx+1, l.firstIndex())
	return l.committed+1 > offet
}

func (l *raftLog) nextEntsSince(sinceIdx uint64) (ents []raftpb.Entry) {
	offset := max(sinceIdx+1, l.firstIndex())
	if l.committed+1 > offset {
		ents, err := l.slice(offset, l.committed+1, noLimit)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

func (rn *RawNode) commitReadyAppend(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoftSt = rd.SoftState
	}

	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardSt = rd.HardState
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		rn.raft.raftLog.stableTo(e.Index, e.Term)
	}

	if !IsEmptySnap(rd.Snapshot) {
		rn.raft.raftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}

	if len(rd.ReadStates) != 0 {
		rn.raft.readStates = nil
	}
}

func (rn *RawNode) commitApply(applied uint64) {
	rn.raft.raftLog.appliedTo(applied)
}

func newReadyWithSinceIdx(r *raft, prevSoftSt *SoftState, prevHardSt raftpb.HardState, sinceIdx uint64) Ready {
	rd := Ready{
		Entries:  r.raftLog.unstableEntries(),
		Messages: r.msgs,
	}
	if sinceIdx == 0 {
		rd.CommittedEntries = r.raftLog.nextEnts()
	} else {
		rd.CommittedEntries = r.raftLog.nextEntsSince(sinceIdx)
	}

	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}

	r.msgs = nil
	return rd
}
