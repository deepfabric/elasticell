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
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"golang.org/x/net/context"
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

	peerHeartbeatsMap *peerHeartbeatsMap
	pendingReads      *readIndexQueue
	proposals         *proposalQueue

	lastHBJob *util.Job

	writtenKeys    uint64
	writtenBytes   uint64
	sizeDiffHint   uint64
	deleteKeysHint uint64
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
		cellID:   cell.ID,
		reads:    queue.NewRingBuffer(defaultQueueSize),
		readyCnt: 0,
	}
	pr.peerHeartbeatsMap = newPeerHeartbeatsMap()
	pr.proposals = newProposalQueue()

	store.runner.RunCancelableTask(pr.doSendFromChan)

	// If this region has only one peer and I am the one, campaign directly.
	if len(cell.Peers) == 1 && cell.Peers[0].StoreID == store.id {
		err = rn.Campaign(context.TODO())
		if err != nil {
			return nil, err
		}
	}

	return pr, nil
}

func (pr *PeerReplicate) doHeartbeat() error {
	req := new(pdpb.CellHeartbeatReq)
	req.Cell = pr.getCell()
	req.Leader = &pr.peer
	req.DownPeers = pr.collectDownPeers(pr.store.cfg.getMaxPeerDownSecDuration())
	req.PendingPeers = pr.collectPendingPeers()

	rsp, err := pr.store.pdClient.CellHeartbeat(context.TODO(), req)
	if err != nil {
		log.Errorf("heartbeat-cell[%d]: send cell heartbeat failed, errors:\n %+v",
			pr.cellID,
			err)
		return err
	}

	if rsp.ChangePeer != nil {
		log.Infof("heartbeat-cell[%d]: try to change peer, type=<%v> peer=<%+v>",
			pr.cellID,
			rsp.ChangePeer.Type,
			rsp.ChangePeer.Peer)

		if nil == rsp.ChangePeer.Peer {
			log.Fatal("bug: peer can not be nil")
		}

		adminReq := newChangePeerRequest(rsp.ChangePeer.Type, *rsp.ChangePeer.Peer)
		pr.store.sendAdminRequest(pr.getCell(), pr.peer, adminReq)
	}

	return nil
}

func (pr *PeerReplicate) checkPeers() {
	if !pr.isLeader() {
		pr.peerHeartbeatsMap.clear()
		return
	}

	peers := pr.getCell().Peers
	if pr.peerHeartbeatsMap.size() == len(peers) {
		return
	}

	// Insert heartbeats in case that some peers never response heartbeats.
	for _, p := range peers {
		pr.peerHeartbeatsMap.putOnlyNotExist(p.ID, time.Now())
	}
}

func (pr *PeerReplicate) collectDownPeers(maxDuration time.Duration) []pdpb.PeerStats {
	now := time.Now()
	var downPeers []pdpb.PeerStats
	for _, p := range pr.getCell().Peers {
		if p.ID == pr.cellID {
			continue
		}

		if pr.peerHeartbeatsMap.has(p.ID) {
			last := pr.peerHeartbeatsMap.get(p.ID)
			if now.Sub(last) >= maxDuration {
				state := pdpb.PeerStats{}
				state.Peer = *p
				state.DownSeconds = uint64(now.Sub(last).Seconds())

				downPeers = append(downPeers, state)
			}
		}
	}
	return downPeers
}

func (pr *PeerReplicate) collectPendingPeers() []metapb.Peer {
	var pendingPeers []metapb.Peer
	status := pr.rn.Status()
	truncatedIdx := pr.getStore().getTruncatedIndex()

	for id, progress := range status.Progress {
		if id == pr.peer.ID {
			continue
		}

		if progress.Match < truncatedIdx {
			p, ok := pr.store.peerCache.get(id)
			if ok {
				pendingPeers = append(pendingPeers, p)
			}
		}
	}

	return pendingPeers
}

func newChangePeerRequest(changeType pdpb.ConfChangeType, peer metapb.Peer) *raftcmdpb.AdminRequest {
	req := new(raftcmdpb.AdminRequest)
	req.Type = raftcmdpb.ChangePeer

	subReq := new(raftcmdpb.ChangePeerRequest)
	subReq.ChangeType = changeType
	subReq.Peer = peer
	req.Body = util.MustMarshal(subReq)

	return req
}

func (pr *PeerReplicate) getStore() *peerStorage {
	return pr.ps
}

func (pr *PeerReplicate) getCell() metapb.Cell {
	return pr.getStore().getCell()
}
