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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
)

const (
	defaultJobWorkerCnt = 1
	defaultJobQueueCap  = 10000
)

// Store is the store for raft
type Store struct {
	id            uint64
	trans         *transport
	engine        storage.Driver
	cfg           *Cfg
	replicatesMap *cellPeersMap // cellid -> peer replicate
	keyRanges     *util.CellTree
	peerCache     *peerCacheMap
	pendingCells  []metapb.Cell
	taskRunner    *util.TaskRunner
}

// NewStore returns store
func NewStore(id uint64, engine storage.Driver, cfg *Cfg) *Store {
	s := new(Store)
	s.id = id
	s.engine = engine
	s.cfg = cfg
	s.trans = newTransport(s.onRaftMessage)
	s.replicatesMap = newCellPeersMap()
	s.keyRanges = util.NewCellTree()
	s.peerCache = newPeerCacheMap()
	s.taskRunner = util.NewTaskRunner(defaultJobWorkerCnt, defaultJobQueueCap)

	return s
}

// Start returns the error when start store
func (s *Store) Start() error {
	return s.trans.start()
}

func (s *Store) onRaftMessage(msg *mraft.RaftMessage) {
	if log.DebugEnabled() {
		log.Debugf("raftstore[store-%d]: receive a raft message, fromPeer=<%d> toPeer=<%d> msg=<%s>",
			s.id,
			msg.FromPeer.ID,
			msg.ToPeer.ID,
			msg.String())
	}

	if !s.isRaftMsgValid(msg) {
		return
	}

	if msg.IsTombstone {
		// we receive a message tells us to remove ourself.
		s.handleGCPeerMsg(msg)
		return
	}

	yes, err := s.isMsgStale(msg)
	if err != nil || yes {
		return
	}

	if !s.tryToCreatePeerReplicate(msg.CellID, msg) {
		return
	}

	ok, err := s.checkSnapshot(msg)
	if err != nil {
		return
	}
	if !ok {
		return
	}

	s.addPeerToCache(msg.FromPeer)

	pr := s.getPeerReplicate(msg.CellID)
	err = pr.step(msg.Message)
	if err != nil {
		return
	}
}

func (s *Store) handleGCPeerMsg(msg *mraft.RaftMessage) {
	// TODO: impl
}

func (s *Store) handleStaleMsg(msg *mraft.RaftMessage, currEpoch metapb.CellEpoch, needGC bool) {
	// TODO: impl
}

// If target peer doesn't exist, create it.
//
// return false to indicate that target peer is in invalid state or
// doesn't exist and can't be created.
func (s *Store) tryToCreatePeerReplicate(cellID uint64, msg *mraft.RaftMessage) bool {
	var (
		hasPeer     = false
		asyncRemove = false
		stalePeer   metapb.Peer
	)

	target := msg.ToPeer

	if p := s.getPeerReplicate(cellID); p != nil {
		hasPeer = true

		// we may encounter a message with larger peer id, which means
		// current peer is stale, then we should remove current peer
		if p.peer.ID < target.ID {
			// cancel snapshotting op
			if p.ps.isApplyingSnapshot() && !p.ps.cancelApplyingSnapJob() {
				log.Infof("raftstore[cell-%d]: stale peer is applying snapshot, will destroy next time, peer=<%d>",
					cellID,
					p.peer.ID)

				return false
			}

			stalePeer = p.peer
			asyncRemove = p.getStore().isInitialized()
		} else if p.peer.ID > target.ID {
			log.Infof("raftstore[cell-%d]: may be from peer is stale, targetID=<%d> currentID=<%d>",
				cellID,
				target.ID,
				p.peer.ID)
			return false
		}
	}

	// If we found stale peer, we will destory it
	if stalePeer.ID > 0 {
		if asyncRemove {
			s.destroyPeer(cellID, stalePeer, asyncRemove)
			return false
		}

		s.destroyPeer(cellID, stalePeer, false)
		hasPeer = false
	}

	if hasPeer {
		return true
	}

	// arrive here means target peer not found, we will try to create it
	message := msg.Message
	if message.Type != raftpb.MsgVote &&
		(message.Type != raftpb.MsgHeartbeat || message.Commit != invalidIndex) {
		log.Debugf("raftstore[cell-%d]: target peer doesn't exist, peer=<%s> message=<%s>",
			cellID,
			target,
			message.Type)
		return false
	}

	// check range overlapped
	item := s.keyRanges.Search(msg.Start)
	if item.ID > 0 {
		if bytes.Compare(encStartKey(item), getDataEndKey(msg.End)) < 0 {
			if log.DebugEnabled() {
				log.Debugf("raftstore[cell-%d]: msg is overlapped with cell, cell=<%s> msg=<%s>",
					cellID,
					item.String(),
					msg.String())
			}
			return false
		}
	}

	// now we can create a replicate
	peerReplicate, err := doReplicate(s, cellID, target.ID)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: replicate peer failure, errors:\n %+v",
			cellID,
			err)
		return false
	}

	// following snapshot may overlap, should insert into keyRanges after
	// snapshot is applied.
	s.keyRanges.Update(peerReplicate.getCell())
	return true
}

func (s *Store) destroyPeer(cellID uint64, target metapb.Peer, async bool) {
	log.Infof("raftstore[cell-%d]: destroying stal peer, peer=<%v> async=<%s>",
		cellID,
		target,
		async)
}

func (s *Store) getPeerReplicate(cellID uint64) *PeerReplicate {
	return s.replicatesMap.get(cellID)
}

func (s *Store) addPeerToCache(peer metapb.Peer) {
	s.peerCache.put(peer.ID, peer)
}

func (s *Store) addJob(task func() error) (*util.Job, error) {
	return s.taskRunner.RunJob(task)
}
