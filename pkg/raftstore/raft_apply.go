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
	"sync"

	"bytes"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
)

type asyncApplyResult struct {
	cellID           uint64
	appliedIndexTerm uint64
	applyState       mraft.RaftApplyState
	result           *execResult
}

type execResult struct {
}

type pendingCmd struct {
	uuid []byte
	term uint64
	cb   func(*raftcmdpb.RaftCMDResponse)
}

type applyDelegate struct {
	sync.RWMutex

	store *Store

	peerID uint64
	cell   metapb.Cell

	// if we remove ourself in ChangePeer remove, we should set this flag, then
	// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	applyState       mraft.RaftApplyState
	appliedIndexTerm uint64
	term             uint64

	pendingCmds          []*pendingCmd
	pendingChangePeerCMD *pendingCmd
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	d.Lock()
	for _, cmd := range d.pendingCmds {
		d.notifyStaleCMD(cmd)
	}

	if nil != d.pendingChangePeerCMD {
		d.notifyStaleCMD(d.pendingChangePeerCMD)
	}
	d.Unlock()
}

func (d *applyDelegate) findCB(uuid []byte, term uint64, req *raftcmdpb.RaftCMDRequest) func(*raftcmdpb.RaftCMDResponse) {
	if isChangePeerCMD(req) {
		cmd := d.getPendingChangePeerCMD()
		if cmd == nil {
			return nil
		} else if bytes.Compare(uuid, cmd.uuid) == 0 {
			return cmd.cb
		}

		d.notifyStaleCMD(cmd)
		return nil
	}

	for {
		head := d.popPendingCMD(term)
		if head == nil {
			return nil
		}

		if bytes.Compare(head.uuid, uuid) == 0 {
			return head.cb
		}

		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		d.notifyStaleCMD(head)
	}
}

func (d *applyDelegate) setPedingChangePeerCMD(cmd *pendingCmd) {
	d.Lock()
	d.pendingChangePeerCMD = cmd
	d.Unlock()
}

func (d *applyDelegate) getPendingChangePeerCMD() *pendingCmd {
	d.RLock()
	cmd := d.pendingChangePeerCMD
	d.RLock()

	return cmd
}

func (d *applyDelegate) popPendingCMD(staleTerm uint64) *pendingCmd {
	d.Lock()
	if len(d.pendingCmds) == 0 {
		d.Unlock()
		return nil
	}

	if d.pendingCmds[0].term > staleTerm {
		d.Unlock()
		return nil
	}

	c := d.pendingCmds[0]
	d.pendingCmds[0] = nil
	d.pendingCmds = d.pendingCmds[1:]
	d.Unlock()
	return c
}

func isChangePeerCMD(req *raftcmdpb.RaftCMDRequest) bool {
	return nil != req.AdminRequest &&
		req.AdminRequest.Type == raftcmdpb.ChangePeer
}

func (d *applyDelegate) notifyStaleCMD(cmd *pendingCmd) {
	resp := errorStaleCMDResp(cmd.uuid, d.term)
	log.Infof("raftstore-apply[cell-%d]: cmd is stale, skip. cmd=<%+v>", d.cell.ID, cmd)
	cmd.cb(resp)
}

func (d *applyDelegate) applyCommittedEntries(commitedEntries []raftpb.Entry) {
	if len(commitedEntries) <= 0 {
		return
	}

	for _, entry := range commitedEntries {
		if d.isPendingRemove() {
			// This peer is about to be destroyed, skip everything.
			break
		}

		expectIndex := d.applyState.AppliedIndex + 1
		if expectIndex != entry.Index {
			log.Fatalf("raftstore-apply[cell-%d]: index not match, expect=<%d> get=<%d>",
				d.cell.ID,
				expectIndex,
				entry.Index)
		}

		var result *execResult

		switch entry.Type {
		case raftpb.EntryNormal:
			result = d.applyEntry(&entry)
		case raftpb.EntryConfChange:
			result = d.applyConfChange(&entry)
		}

		pr := d.store.replicatesMap.get(d.cell.ID)
		if pr != nil {
			pr.doPostApply(&asyncApplyResult{
				cellID:           d.cell.ID,
				appliedIndexTerm: d.appliedIndexTerm,
				applyState:       d.applyState,
				result:           result,
			})
		}
	}
}

func (d *applyDelegate) applyEntry(entry *raftpb.Entry) *execResult {
	if len(entry.Data) > 0 {
		req := &raftcmdpb.RaftCMDRequest{}
		util.MustUnmarshal(req, entry.Data)
		return d.doApplyRaftCMD(req, entry.Term, entry.Index)
	}

	// when a peer become leader, it will send an empty entry.
	state := d.applyState
	state.AppliedIndex = entry.Index

	err := d.store.engine.Set(getApplyStateKey(d.cell.ID), util.MustMarshal(&state))
	if err != nil {
		log.Fatalf("raftstore-apply[cell-%d]: apply empty entry failed, entry=<%s> errors:\n %+v",
			d.cell.ID,
			entry.String(),
			err)
	}

	d.applyState = state
	d.appliedIndexTerm = entry.Term
	if entry.Term <= 0 {
		panic("error empty entry term.")
	}

	for {
		cmd := d.popPendingCMD(entry.Term - 1)
		if cmd == nil {
			return nil
		}

		// apprently, all the callbacks whose term is less than entry's term are stale.
		d.notifyStaleCMD(cmd)
	}
}

func (d *applyDelegate) applyConfChange(entry *raftpb.Entry) *execResult {
	// TODO: impl
	return nil
}

func (d *applyDelegate) destroy() {
	//TODO: impl
}

func (d *applyDelegate) setPendingRemove() {
	d.Lock()
	d.pendingRemove = true
	d.Unlock()
}

func (d *applyDelegate) isPendingRemove() bool {
	d.RLock()
	value := d.pendingRemove
	d.RUnlock()

	return value
}
