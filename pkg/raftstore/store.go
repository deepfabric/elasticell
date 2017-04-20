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

	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"golang.org/x/net/context"
)

var (
	defaultJobQueueCap uint64 = 10000
)

// Store is the store for raft
type Store struct {
	cfg *Cfg

	id        uint64
	clusterID uint64
	meta      metapb.Store

	pdClient *pd.Client

	replicatesMap *cellPeersMap // cellid -> peer replicate
	keyRanges     *util.CellTree
	peerCache     *peerCacheMap
	delegates     *applyDelegateMap
	pendingCells  []metapb.Cell

	trans  *transport
	engine storage.Driver
	runner *util.Runner
}

// NewStore returns store
func NewStore(clusterID uint64, pdClient *pd.Client, meta metapb.Store, engine storage.Driver, cfg *Cfg) *Store {
	s := new(Store)
	s.clusterID = clusterID
	s.id = meta.ID
	s.meta = meta
	s.engine = engine
	s.cfg = cfg
	s.pdClient = pdClient

	s.trans = newTransport(s.cfg.Raft, s.onRaftMessage)

	s.keyRanges = util.NewCellTree()
	s.replicatesMap = newCellPeersMap()
	s.peerCache = newPeerCacheMap()
	s.delegates = newApplyDelegateMap()

	s.runner = util.NewRunner()

	s.init()

	return s
}

func (s *Store) init() {
	totalCount := 0
	tomebstoneCount := 0
	applyingCount := 0

	err := s.engine.Scan(cellMetaMinKey, cellMetaMaxKey, func(key, value []byte) (bool, error) {
		cellID, suffix, err := decodeCellMetaKey(key)
		if err != nil {
			return false, err
		}

		if suffix != cellStateSuffix {
			return true, nil
		}

		totalCount++

		localState := new(mraft.CellLocalState)
		util.MustUnmarshal(localState, value)

		if localState.State == mraft.Tombstone {
			tomebstoneCount++
			log.Infof("bootstrap: cell is tombstone in store, cellID=<%d>", cellID)
			return true, nil
		}

		pr, err := createPeerReplicate(s, &localState.Cell)
		if err != nil {
			return false, err
		}

		if localState.State == mraft.Applying {
			applyingCount++
			log.Infof("bootstrap: cell is applying in store, cellID=<%d>", cellID)
			pr.startApplyingSnapJob()
		}

		s.keyRanges.Update(localState.Cell)
		s.replicatesMap.put(cellID, pr)

		return true, nil
	})

	if err != nil {
		log.Fatalf("bootstrap: init store failed, errors:\n %+v", err)
	}

	log.Infof("bootstrap: starts with %d cells, including %d tombstones and %d applying	cells",
		totalCount,
		tomebstoneCount,
		applyingCount)

	s.cleanup()
}

func (s *Store) cleanup() {
	// clean up all possible garbage data
	lastStartKey := getDataKey([]byte(""))

	s.keyRanges.Ascend(func(cell *metapb.Cell) bool {
		start := encStartKey(cell)
		err := s.engine.RangeDelete(lastStartKey, start)
		if err != nil {
			log.Fatalf("bootstrap: cleanup possible garbage data failed, start=<%v> end=<%v> errors:\n %+v",
				lastStartKey,
				start,
				err)
		}

		lastStartKey = encEndKey(cell)
		return true
	})

	err := s.engine.RangeDelete(lastStartKey, dataMaxKey)
	if err != nil {
		log.Fatalf("bootstrap: cleanup possible garbage data failed, start=<%v> end=<%v> errors:\n %+v",
			lastStartKey,
			dataMaxKey,
			err)
	}

	log.Infof("bootstrap: cleanup possible garbage data complete")
}

// Start returns the error when start store
func (s *Store) Start() {
	// TODO: impl
	go s.startTransfer()
	<-s.trans.server.Started()

	s.startStoreHeartbeatTask()
	s.startCellHeartbeatTask()
	s.startGCTask()
	s.startSplitCheckTask()
}

func (s *Store) startTransfer() {
	err := s.trans.start()
	if err != nil {
		log.Fatalf("bootstrap: start transfer failed, errors:\n %+v", err)
	}
}

func (s *Store) startStoreHeartbeatTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.getStoreHeartbeatDuration())

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.handleStoreHeartbeat()
			}
		}
	})
}

func (s *Store) startCellHeartbeatTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.getCellHeartbeatDuration())

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.handleCellHeartbeat()
			}
		}
	})
}

func (s *Store) startGCTask() {
	// TODO: impl
}

func (s *Store) startSplitCheckTask() {
	// TODO: impl
}

// Stop returns the error when stop store
func (s *Store) Stop() error {
	err := s.runner.Stop()
	s.trans.stop()
	return err
}

// GetID returns store id
func (s *Store) GetID() uint64 {
	return s.id
}

// GetMeta returns store meta
func (s *Store) GetMeta() metapb.Store {
	return s.meta
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
		// TODO: destroy async
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
		if bytes.Compare(encStartKey(&item), getDataEndKey(msg.End)) < 0 {
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

	// TODO: impl
}

func (s *Store) getPeerReplicate(cellID uint64) *PeerReplicate {
	return s.replicatesMap.get(cellID)
}

func (s *Store) addPeerToCache(peer metapb.Peer) {
	s.peerCache.put(peer.ID, peer)
}

func (s *Store) addJob(task func() error) (*util.Job, error) {
	return s.runner.RunJob(task)
}

func (s *Store) handleStoreHeartbeat() {
	req := new(pdpb.StoreHeartbeatReq)
	req.Header.ClusterID = s.clusterID
	// TODO: impl
	req.Stats = &pdpb.StoreStats{
		StoreID:            s.GetID(),
		Capacity:           0,
		Available:          0,
		CellCount:          s.replicatesMap.size(),
		SendingSnapCount:   0,
		ReceivingSnapCount: 0,
		StartTime:          0,
		ApplyingSnapCount:  0,
		IsBusy:             false,
		UsedSize:           0,
		BytesWritten:       0,
	}

	_, err := s.pdClient.StoreHeartbeat(context.TODO(), req)
	if err != nil {
		log.Errorf("heartbeat-store[%d]: heartbeat failed, errors:\n +%v",
			s.id,
			err)
	}
}

func (s *Store) handleCellHeartbeat() {
	for _, p := range s.replicatesMap.values() {
		p.sendHeartbeat(s.clusterID)
	}
}
