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
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

const (
	msgChanBuf       = 1024
	defaultQueueSize = 1024
)

// PeerReplicate is the cell's peer replicate. Every cell replicate has a PeerReplicate.
type PeerReplicate struct {
	cellID     uint64
	peer       metapb.Peer
	raftTicker *time.Ticker
	rn         raft.Node
	store      *Store
	ps         *peerStorage
	msgC       chan raftpb.Message

	pendingReads *readIndexQueue
}

func createPeerReplicate(store *Store, cell *metapb.Cell) (*PeerReplicate, error) {
	peer := findPeer(*cell, store.GetID())
	if peer == nil {
		return nil, fmt.Errorf("bootstrap: find no peer for store in cell. storeID=<%d> cell=<%+v>",
			store.GetID(),
			cell)
	}

	return newPeerReplicate(store, cell, peer.ID)
}

// The peer can be created from another node with raft membership changes, and we only
// know the cell_id and peer_id when creating this replicated peer, the cell info
// will be retrieved later after applying snapshot.
func doReplicate(store *Store, cellID, peerID uint64) (*PeerReplicate, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("raftstore[cell-%d]: replicate peer, peerID=<%d>",
		cellID,
		peerID)

	cell := &metapb.Cell{
		ID: cellID,
	}

	return newPeerReplicate(store, cell, peerID)
}

func newPeerReplicate(store *Store, cell *metapb.Cell, peerID uint64) (*PeerReplicate, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("invalid peer id: %d", peerID)
	}

	ps, err := newPeerStorage(store, *cell)
	if err != nil {
		return nil, err
	}

	pr := new(PeerReplicate)
	pr.peer = newPeer(peerID, store.id)
	pr.cellID = cell.ID
	pr.ps = ps

	c := getRaftConfig(peerID, ps.getAppliedIndex(), ps, store.cfg.Raft)
	rn := raft.StartNode(c, nil)
	pr.rn = rn
	pr.raftTicker = time.NewTicker(time.Millisecond * time.Duration(store.cfg.Raft.BaseTick))
	pr.msgC = make(chan raftpb.Message, msgChanBuf)
	pr.store = store
	pr.pendingReads = &readIndexQueue{
		reads:    queue.NewRingBuffer(defaultQueueSize),
		readyCnt: 0,
	}

	store.runner.RunCancelableTask(pr.doSendFromChan)
	return pr, nil
}

func (pr *PeerReplicate) getStore() *peerStorage {
	return pr.ps
}

func (pr *PeerReplicate) getCell() metapb.Cell {
	return pr.getStore().getCell()
}

func (pr *PeerReplicate) sendHeartbeat(clusterID uint64) {
	// TODO: impl
}
