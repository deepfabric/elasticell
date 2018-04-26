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
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pilosa/pilosa"
	"golang.org/x/net/context"
)

type action int

const (
	checkSplit = iota
	checkCompact
	doCampaign
)

// PeerReplicate is the cell's peer replicate. Every cell replicate has a PeerReplicate.
type PeerReplicate struct {
	cellID            uint64
	peer              metapb.Peer
	rn                *raft.RawNode
	store             *Store
	ps                *peerStorage
	batch             *proposeBatch
	events            *queue.RingBuffer
	ticks             *util.Queue
	steps             *util.Queue
	reports           *util.Queue
	applyResults      *util.Queue
	requests          *util.Queue
	actions           *util.Queue
	stopRaftTick      bool
	peerHeartbeatsMap *peerHeartbeatsMap
	pendingReads      *readIndexQueue
	lastHBJob         *util.Job
	writtenKeys       uint64
	writtenBytes      uint64
	sizeDiffHint      uint64
	raftLogSizeHint   uint64
	deleteKeysHint    uint64
	cancelTaskIds     []uint64
	metrics           localMetrics
	nextDocID         uint64
}

func createPeerReplicate(store *Store, cell *metapb.Cell) (*PeerReplicate, error) {
	peer := findPeer(cell, store.GetID())
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
func doReplicate(store *Store, msg *mraft.RaftMessage, peerID uint64) (*PeerReplicate, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("raftstore[cell-%d]: replicate peer, peerID=<%d>",
		msg.CellID,
		peerID)

	cell := &metapb.Cell{
		ID:    msg.CellID,
		Epoch: msg.CellEpoch,
		Start: msg.Start,
		End:   msg.End,
	}

	return newPeerReplicate(store, cell, peerID)
}

func newPeerReplicate(store *Store, cell *metapb.Cell, peerID uint64) (*PeerReplicate, error) {
	if peerID == pd.ZeroID {
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
	pr.batch = newBatch(pr)

	c := getRaftConfig(peerID, ps.getAppliedIndex(), ps)
	rn, err := raft.NewRawNode(c, nil)
	if err != nil {
		return nil, err
	}

	pr.rn = rn
	pr.events = queue.NewRingBuffer(2)
	pr.ticks = &util.Queue{}
	pr.steps = &util.Queue{}
	pr.reports = &util.Queue{}
	pr.applyResults = &util.Queue{}
	pr.requests = &util.Queue{}
	pr.actions = &util.Queue{}

	pr.store = store
	pr.pendingReads = &readIndexQueue{
		cellID:   cell.ID,
		readyCnt: 0,
	}
	pr.peerHeartbeatsMap = newPeerHeartbeatsMap()

	// If this region has only one peer and I am the one, campaign directly.
	if len(cell.Peers) == 1 && cell.Peers[0].StoreID == store.id {
		err = rn.Campaign()
		if err != nil {
			return nil, err
		}

		log.Debugf("raft-cell[%d]: try to campaign leader",
			pr.cellID)
	}

	id, _ := store.runner.RunCancelableTask(pr.readyToServeRaft)
	pr.cancelTaskIds = append(pr.cancelTaskIds, id)

	return pr, nil
}

func (pr *PeerReplicate) maybeCampaign() (bool, error) {
	if len(pr.ps.cell.Peers) <= 1 {
		// The peer campaigned when it was created, no need to do it again.
		return false, nil
	}

	err := pr.rn.Campaign()
	if err != nil {
		return false, err
	}

	log.Debugf("raft-cell[%d]: try to campaign leader",
		pr.cellID)
	return true, nil
}

func (pr *PeerReplicate) onAdminRequest(adminReq *raftcmdpb.AdminRequest) {
	r := acquireReqCtx()
	r.admin = adminReq
	pr.addRequest(r)
	commandCounterVec.WithLabelValues(raftcmdpb.CMDType_name[int32(adminReq.Type)]).Inc()
}

func (pr *PeerReplicate) onReq(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) {
	if globalCfg.EnableMetricsRequest {
		now := time.Now().UnixNano()
		req.StartAt = now
		req.LastStageAt = now
	}
	commandCounterVec.WithLabelValues(raftcmdpb.CMDType_name[int32(req.Type)]).Inc()

	var start time.Time
	if globalCfg.EnableMetricsRequest {
		start = time.Now()
	}

	r := acquireReqCtx()
	r.req = req
	r.cb = cb
	pr.addRequest(r)

	if globalCfg.EnableMetricsRequest {
		observeRequestInQueue(start)
	}

	if log.DebugEnabled() {
		log.Debugf("req: added to cell req queue. uuid=<%d>, cell=<%d>",
			req.UUID,
			pr.cellID)
	}
}

func (pr *PeerReplicate) addAction(act action) {
	err := pr.actions.Put(act)
	if err != nil {
		return
	}

	pr.addEvent()
}

func (pr *PeerReplicate) addRequest(req *reqCtx) {
	err := pr.requests.Put(req)
	if err != nil {
		if req.cb != nil {
			pr.store.respStoreNotMatch(errStoreNotMatch, req.req, req.cb)
		}
		pool.ReleaseRequest(req.req)
		return
	}

	pr.addEvent()
}

func (pr *PeerReplicate) resetBatch() {
	pr.batch = newBatch(pr)
}

func (pr *PeerReplicate) handleHeartbeat() {
	var err error
	if pr.isLeader() {
		if pr.lastHBJob != nil && pr.lastHBJob.IsNotComplete() {
			// cancel last if not complete
			pr.lastHBJob.Cancel()
		}

		err = pr.store.addPDJob(pr.doHeartbeat, pr.setLastHBJob)
		if err != nil {
			log.Errorf("heartbeat-cell[%d]: add cell heartbeat job failed, errors:\n %+v",
				pr.cellID,
				err)
			pr.lastHBJob = nil
		}
	}
}

func (pr *PeerReplicate) setLastHBJob(job *util.Job) {
	pr.lastHBJob = job
}

func (pr *PeerReplicate) doHeartbeat() error {
	req := new(pdpb.CellHeartbeatReq)
	req.Cell = pr.getCell()
	req.Leader = &pr.peer
	req.DownPeers = pr.collectDownPeers(globalCfg.LimitPeerDownDuration)
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

		pr.onAdminRequest(newChangePeerRequest(rsp.ChangePeer.Type, *rsp.ChangePeer.Peer))
	} else if rsp.TransferLeader != nil {
		log.Infof("heartbeat-cell[%d]: try to transfer leader, from=<%v> to=<%+v>",
			pr.cellID,
			pr.peer.ID,
			rsp.TransferLeader.Peer.ID)
		pr.onAdminRequest(newTransferLeaderRequest(rsp.TransferLeader))
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
		if p.ID == pr.peer.ID {
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

func (pr *PeerReplicate) stopEventLoop() {
	pr.events.Dispose()
}

func (pr *PeerReplicate) destroy() error {
	log.Infof("raftstore-destroy[cell-%d]: begin to destroy",
		pr.cellID)

	pr.stopEventLoop()

	pr.store.removePendingSnapshot(pr.cellID)
	pr.store.removeDroppedVoteMsg(pr.cellID)

	wb := pr.store.engine.NewWriteBatch()

	err := pr.store.clearMeta(pr.cellID, wb)
	if err != nil {
		return err
	}

	err = pr.ps.updatePeerState(pr.getCell(), mraft.Tombstone, wb)
	if err != nil {
		return err
	}

	err = pr.store.engine.Write(wb, false)
	if err != nil {
		return err
	}

	if pr.ps.isInitialized() {
		err := pr.ps.clearData()
		if err != nil {
			log.Errorf("raftstore-destroy[cell-%d]: add clear data job failed, errors:\n %+v",
				pr.cellID,
				err)
			return err
		}
	}

	for _, id := range pr.cancelTaskIds {
		pr.store.runner.StopCancelableTask(id)
	}

	log.Infof("raftstore-destroy[cell-%d]: destroy self complete.",
		pr.cellID)

	return nil
}

func (pr *PeerReplicate) getStore() *peerStorage {
	return pr.ps
}

func (pr *PeerReplicate) getCell() metapb.Cell {
	return pr.getStore().getCell()
}

func (pr *PeerReplicate) getPeer() metapb.Peer {
	return pr.peer
}

// AllocateDocID allocates a docID which is unique among the cluster.
// Note that the same document has different docID on multiple replicas if inserted separatly, has smae docID if a replica is initialized from a snapshot of another.
func (pr *PeerReplicate) AllocateDocID() (docID uint64, err error) {
	if pr.nextDocID&(pilosa.SliceWidth-1) == 0 {
		// the first time to allocate, or current allocated SliceWidth is used up
		var rsp *pdpb.AllocIDRsp
		if rsp, err = pr.store.pdClient.AllocID(context.TODO(), new(pdpb.AllocIDReq)); err != nil {
			return
		}
		docID = rsp.GetID() * pilosa.SliceWidth
		pr.nextDocID = docID + 1
		log.Infof("peer.go[cell-%d]: batch allocated docID [%v, %v)", pr.cellID, docID, docID+pilosa.SliceWidth)
	} else {
		docID = pr.nextDocID
		pr.nextDocID++
	}
	return
}
