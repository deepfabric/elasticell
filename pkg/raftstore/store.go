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
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var (
	defaultJobQueueCap uint64 = 10000
)

const (
	applyWorker            = "apply-work"
	snapWorker             = "snap-work"
	pdWorker               = "pd-work"
	splitWorker            = "split-work"
	raftGCWorker           = "raft-gc-worker"
	defaultWorkerQueueSize = 64
	defaultNotifyChanSize  = 1024
)

// Store is the store for raft
type Store struct {
	cfg *Cfg

	id        uint64
	clusterID uint64
	startAt   uint32
	meta      metapb.Store

	snapshotManager SnapshotManager

	pdClient *pd.Client

	replicatesMap *cellPeersMap // cellid -> peer replicate
	keyRanges     *util.CellTree
	peerCache     *peerCacheMap
	delegates     *applyDelegateMap
	pendingCells  []metapb.Cell

	trans      *transport
	engine     storage.Driver
	runner     *util.Runner
	notifyChan chan interface{}

	redisReadHandles  map[raftcmdpb.CMDType]func(*raftcmdpb.Request) *raftcmdpb.Response
	redisWriteHandles map[raftcmdpb.CMDType]func(*execContext, *raftcmdpb.Request) *raftcmdpb.Response

	sendingSnapCount   uint32
	reveivingSnapCount uint32
}

// NewStore returns store
func NewStore(clusterID uint64, pdClient *pd.Client, meta metapb.Store, engine storage.Driver, cfg *Cfg) *Store {
	s := new(Store)
	s.clusterID = clusterID
	s.id = meta.ID
	s.meta = meta
	s.startAt = uint32(time.Now().Unix())
	s.engine = engine
	s.cfg = cfg
	s.pdClient = pdClient
	s.snapshotManager = newDefaultSnapshotManager(cfg, engine.GetDataEngine())

	s.trans = newTransport(s, pdClient, s.notify)
	s.notifyChan = make(chan interface{}, defaultNotifyChanSize)

	s.keyRanges = util.NewCellTree()
	s.replicatesMap = newCellPeersMap()
	s.peerCache = newPeerCacheMap()
	s.delegates = newApplyDelegateMap()

	s.runner = util.NewRunner()
	s.runner.AddNamedWorker(snapWorker, defaultWorkerQueueSize)
	s.runner.AddNamedWorker(applyWorker, defaultWorkerQueueSize)
	s.runner.AddNamedWorker(pdWorker, defaultWorkerQueueSize)
	s.runner.AddNamedWorker(splitWorker, defaultWorkerQueueSize)

	s.redisReadHandles = make(map[raftcmdpb.CMDType]func(*raftcmdpb.Request) *raftcmdpb.Response)
	s.redisWriteHandles = make(map[raftcmdpb.CMDType]func(*execContext, *raftcmdpb.Request) *raftcmdpb.Response)

	s.initRedisHandle()
	s.init()

	return s
}

func (s *Store) init() {
	totalCount := 0
	tomebstoneCount := 0
	applyingCount := 0

	wb := s.engine.NewWriteBatch()

	err := s.getMetaEngine().Scan(cellMetaMinKey, cellMetaMaxKey, func(key, value []byte) (bool, error) {
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

		for _, p := range localState.Cell.Peers {
			s.peerCache.put(p.ID, *p)
		}

		if localState.State == mraft.Tombstone {
			s.clearMeta(cellID, wb)
			tomebstoneCount++
			log.Infof("bootstrap: cell is tombstone in store, cellID=<%d>",
				cellID)
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

		pr.startRegistrationJob()

		s.keyRanges.Update(localState.Cell)
		s.replicatesMap.put(cellID, pr)

		return true, nil
	})

	if err != nil {
		log.Fatalf("bootstrap: init store failed, errors:\n %+v", err)
	}

	err = s.engine.Write(wb)
	if err != nil {
		log.Fatalf("bootstrap: init store failed, errors:\n %+v", err)
	}

	log.Infof("bootstrap: starts with %d cells, including %d tombstones and %d applying cells",
		totalCount,
		tomebstoneCount,
		applyingCount)

	s.cleanup()
}

// Start returns the error when start store
func (s *Store) Start() {
	log.Infof("bootstrap: begin to start store %d", s.id)

	go s.startTransfer()
	<-s.trans.server.Started()
	log.Infof("bootstrap: transfer started")

	s.startHandleNotifyMsg()
	log.Infof("bootstrap: ready to handle notify msg")

	s.startStoreHeartbeatTask()
	log.Infof("bootstrap: ready to handle store heartbeat")

	s.startCellHeartbeatTask()
	log.Infof("bootstrap: ready to handle cell heartbeat")

	s.startGCTask()
	log.Infof("bootstrap: ready to handle gc task")

	s.startCellSplitCheckTask()
	log.Infof("bootstrap: ready to handle split check task")
}

func (s *Store) startTransfer() {
	err := s.trans.start()
	if err != nil {
		log.Fatalf("bootstrap: start transfer failed, errors:\n %+v", err)
	}
}

func (s *Store) startHandleNotifyMsg() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: store msg handler stopped")
				return
			case n := <-s.notifyChan:
				if msg, ok := n.(*mraft.RaftMessage); ok {
					s.onRaftMessage(msg)
				} else if msg, ok := n.(*cmd); ok {
					s.onRaftCMD(msg)
				} else if msg, ok := n.(*splitCheckResult); ok {
					s.onSplitCheckResult(msg)
				} else if msg, ok := n.(*mraft.SnapshotData); ok {
					s.snapshotManager.WriteSnapData(msg)
				}
			}
		}
	})
}

func (s *Store) startStoreHeartbeatTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.getStoreHeartbeatDuration())
		defer ticker.Stop()

		var err error
		var job *util.Job

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: store heart beat job stopped")
				return
			case <-ticker.C:
				if job != nil && job.IsNotComplete() {
					// cancel last if not complete
					job.Cancel()
				} else {
					job, err = s.addPDJob(s.handleStoreHeartbeat)
					if err != nil {
						log.Errorf("heartbeat-store[%d]: add job failed, errors:\n %+v",
							s.GetID(),
							err)
					}
				}
			}
		}
	})
}

func (s *Store) startCellHeartbeatTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.getCellHeartbeatDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: cell heart beat job stopped")
				return
			case <-ticker.C:
				s.handleCellHeartbeat()
			}
		}
	})
}

func (s *Store) startGCTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.getRaftGCLogDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: raft log gc job stopped")
				return
			case <-ticker.C:
				s.handleRaftGCLog()
			}
		}
	})
}

func (s *Store) startCellSplitCheckTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.getSplitCellCheckDuration())
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("stop: cell heartbeat job stopped")
				return
			case <-ticker.C:
				s.handleCellSplitCheck()
			}
		}
	})
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

func (s *Store) getTargetCell(key []byte) (*PeerReplicate, error) {
	cell := s.keyRanges.Search(key)
	if cell.ID == pd.ZeroID {
		return nil, errKeyNotInStore
	}

	pr := s.replicatesMap.get(cell.ID)
	if pr == nil {
		return nil, errKeyNotInStore
	}

	return pr, nil
}

// OnProxyReq process proxy req
func (s *Store) OnProxyReq(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) error {
	key := req.Cmd[1]
	pr, err := s.getTargetCell(key)
	if err != nil {
		return err
	}

	cell := pr.getCell()

	raftCMD := new(raftcmdpb.RaftCMDRequest)
	raftCMD.Header = &raftcmdpb.RaftRequestHeader{
		CellId:     cell.ID,
		Peer:       pr.getPeer(),
		ReadQuorum: true, // TODO: configuration
		UUID:       uuid.NewV4().Bytes(),
		CellEpoch:  cell.Epoch,
	}

	req.Cmd[1] = getDataKey(key)

	// TODO: batch process
	raftCMD.Requests = append(raftCMD.Requests, req)
	s.notify(newCMD(raftCMD, cb))
	return nil
}

// OnRedisCommand process redis command
func (s *Store) OnRedisCommand(cmdType raftcmdpb.CMDType, cmd redis.Command, cb func(*raftcmdpb.RaftCMDResponse)) ([]byte, error) {
	if log.DebugEnabled() {
		log.Debugf("raftstore[store-%d]: received a redis command, cmd=<%s>", s.id, cmd.ToString())
	}

	key := cmd.Args()[0]
	pr, err := s.getTargetCell(key)
	if err != nil {
		return nil, err
	}

	cell := pr.getCell()

	req := new(raftcmdpb.RaftCMDRequest)
	req.Header = &raftcmdpb.RaftRequestHeader{
		CellId:     cell.ID,
		Peer:       pr.getPeer(),
		ReadQuorum: true, // TODO: configuration
		UUID:       uuid.NewV4().Bytes(),
		CellEpoch:  cell.Epoch,
	}

	cmd.Args()[0] = getDataKey(key)

	// TODO: batch process
	uuid := uuid.NewV4().Bytes()
	req.Requests = append(req.Requests, &raftcmdpb.Request{
		UUID: uuid,
		Type: cmdType,
		Cmd:  cmd,
	})

	s.notify(newCMD(req, cb))
	return uuid, nil
}

func (s *Store) notify(msg interface{}) {
	s.notifyChan <- msg
}

func (s *Store) onRaftMessage(msg *mraft.RaftMessage) {
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
		log.Errorf("raftstore[store-%d]: check msg stale failed, errors:\n%+v",
			s.id,
			err)
		return
	}

	if !s.tryToCreatePeerReplicate(msg.CellID, msg) {
		log.Warnf("raftstore[store-%d]: try to create peer failed. cell=<%d>",
			s.id,
			msg.CellID)
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

func (s *Store) onRaftCMD(cmd *cmd) {
	// we handle all read, write and admin cmd here
	if cmd.req.Header == nil || cmd.req.Header.UUID == nil {
		cmd.resp(errorOtherCMDResp(errMissingUUIDCMD))
		return
	}

	err := s.validateStoreID(cmd.req)
	if err != nil {
		cmd.respOtherError(err)
		return
	}

	pr := s.replicatesMap.get(cmd.req.Header.CellId)
	term := pr.getCurrentTerm()

	pe := s.validateCell(cmd.req)
	if err != nil {
		cmd.resp(errorPbResp(pe, cmd.req.Header.UUID, term))
		return
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
	meta := &proposalMeta{
		cmd:  cmd,
		uuid: cmd.req.Header.UUID,
		term: term,
	}

	pr.notifyPropose(meta)
}

func (s *Store) onSplitCheckResult(result *splitCheckResult) {
	if len(result.splitKey) == 0 {
		log.Errorf("raftstore-split[cell-%d]: split key must not be empty", result.cellID)
		return
	}

	p := s.replicatesMap.get(result.cellID)
	if p == nil || !p.isLeader() {
		log.Errorf("raftstore-split[cell-%d]: cell not exist or not leader, skip", result.cellID)
		return
	}

	cell := p.getCell()

	if cell.Epoch.CellVer != result.epoch.CellVer {
		log.Infof("raftstore-split[cell-%d]: epoch changed, need re-check later, current=<%+v> split=<%+v>",
			result.cellID,
			cell.Epoch,
			result.epoch)
		return
	}

	err := p.startAskSplitJob(cell, p.peer, result.splitKey)
	if err != nil {
		log.Errorf("raftstore-split[cell-%d]: add ask split job failed, errors:\n %+v",
			result.cellID,
			err)
	}
}

func (s *Store) handleGCPeerMsg(msg *mraft.RaftMessage) {
	cellID := msg.CellID
	needRemove := false
	asyncRemoved := true

	pr := s.replicatesMap.get(cellID)
	if pr != nil {
		fromEpoch := msg.CellEpoch

		if isEpochStale(pr.getCell().Epoch, fromEpoch) {
			log.Infof("raftstore[cell-%d]: receives gc message, remove. msg=<%+v>",
				cellID,
				msg)
			needRemove = true
			asyncRemoved = pr.ps.isInitialized()
		}
	}

	if needRemove {
		s.destroyPeer(cellID, msg.ToPeer, asyncRemoved)
	}
}

func (s *Store) handleStaleMsg(msg *mraft.RaftMessage, currEpoch metapb.CellEpoch, needGC bool) {
	cellID := msg.CellID
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer

	if !needGC {
		log.Infof("raftstore[cell-%d]: raft msg is stale, ignore it, msg=<%+v> current=<%+v>",
			cellID,
			msg,
			currEpoch)
		return
	}

	log.Infof("raftstore[cell-%d]: raft msg is stale, tell to gc, msg=<%+v> current=<%+v>",
		cellID,
		msg,
		currEpoch)

	gc := new(mraft.RaftMessage)
	gc.CellID = cellID
	gc.ToPeer = fromPeer
	gc.FromPeer = toPeer
	gc.CellEpoch = currEpoch
	gc.IsTombstone = true

	err := s.trans.send(fromPeer.StoreID, gc)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: raft msg is stale, send gc msg failed, msg=<%+v> current=<%+v> errors:\n %+v",
			cellID,
			msg,
			currEpoch,
			err)
	}
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
		s.destroyPeer(cellID, stalePeer, asyncRemove)
		if asyncRemove {
			return false
		}

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
	peerReplicate, err := doReplicate(s, msg, target.ID)
	if err != nil {
		log.Errorf("raftstore[cell-%d]: replicate peer failure, errors:\n %+v",
			cellID,
			err)
		return false
	}

	peerReplicate.ps.cell.Peers = append(peerReplicate.ps.cell.Peers, &msg.ToPeer)
	peerReplicate.ps.cell.Peers = append(peerReplicate.ps.cell.Peers, &msg.FromPeer)
	s.keyRanges.Update(peerReplicate.ps.cell)

	// following snapshot may overlap, should insert into keyRanges after
	// snapshot is applied.
	s.replicatesMap.put(cellID, peerReplicate)
	s.addPeerToCache(msg.FromPeer)
	s.addPeerToCache(msg.ToPeer)
	return true
}

func (s *Store) destroyPeer(cellID uint64, target metapb.Peer, async bool) {
	if !async {
		log.Infof("raftstore[cell-%d]: destroying stale peer, peer=<%v>",
			cellID,
			target)

		pr := s.replicatesMap.delete(cellID)
		if pr == nil {
			log.Fatalf("raftstore[cell-%d]: destroy cell not exist", cellID)
		}

		if pr.ps.isApplyingSnap() {
			log.Fatalf("raftstore[cell-%d]: destroy cell is apply for snapshot", cellID)
		}

		err := pr.destroy()
		if err != nil {
			log.Fatalf("raftstore[cell-%d]: destroy cell failed, errors:\n %+v",
				cellID,
				err)
		}

		if pr.ps.isInitialized() && !s.keyRanges.Remove(pr.getCell()) {
			log.Fatalf("raftstore[cell-%d]: remove key range  failed",
				cellID)
		}
	} else {
		log.Infof("raftstore[cell-%d]: asking destroying stale peer, peer=<%v>",
			cellID,
			target)

		s.startDestroyJob(cellID)
	}
}

func (s *Store) cleanup() {
	// clean up all possible garbage data
	lastStartKey := getDataKey([]byte(""))

	s.keyRanges.Ascend(func(cell *metapb.Cell) bool {
		start := encStartKey(cell)
		err := s.getDataEngine().RangeDelete(lastStartKey, start)
		if err != nil {
			log.Fatalf("bootstrap: cleanup possible garbage data failed, start=<%v> end=<%v> errors:\n %+v",
				lastStartKey,
				start,
				err)
		}

		lastStartKey = encEndKey(cell)
		return true
	})

	err := s.getDataEngine().RangeDelete(lastStartKey, dataMaxKey)
	if err != nil {
		log.Fatalf("bootstrap: cleanup possible garbage data failed, start=<%v> end=<%v> errors:\n %+v",
			lastStartKey,
			dataMaxKey,
			err)
	}

	log.Infof("bootstrap: cleanup possible garbage data complete")
}

func (s *Store) clearMeta(cellID uint64, wb storage.WriteBatch) error {
	metaCount := 0
	raftCount := 0

	// meta must in the range [cellID, cellID + 1)
	metaStart := getCellMetaPrefix(cellID)
	metaEnd := getCellMetaPrefix(cellID + 1)

	err := s.getMetaEngine().Scan(metaStart, metaEnd, func(key, value []byte) (bool, error) {
		err := wb.Delete(key)
		if err != nil {
			return false, errors.Wrapf(err, "")
		}

		metaCount++
		return true, nil
	})

	if err != nil {
		return errors.Wrapf(err, "")
	}

	raftStart := getCellRaftPrefix(cellID)
	raftEnd := getCellRaftPrefix(cellID + 1)

	err = s.getMetaEngine().Scan(raftStart, raftEnd, func(key, value []byte) (bool, error) {
		err := wb.Delete(key)
		if err != nil {
			return false, errors.Wrapf(err, "")
		}

		raftCount++
		return true, nil
	})

	if err != nil {
		return errors.Wrapf(err, "")
	}

	log.Infof("raftstore[cell-%d]: clear peer meta keys and raft keys, meta key count=<%d>, raft key count=<%d>",
		cellID,
		metaCount,
		raftCount)

	return nil
}

func (s *Store) getPeerReplicate(cellID uint64) *PeerReplicate {
	return s.replicatesMap.get(cellID)
}

func (s *Store) addPeerToCache(peer metapb.Peer) {
	s.peerCache.put(peer.ID, peer)
}

func (s *Store) addPDJob(task func() error) (*util.Job, error) {
	return s.addNamedJob(pdWorker, task)
}

func (s *Store) addRaftLogGCJob(task func() error) (*util.Job, error) {
	return s.addNamedJob(raftGCWorker, task)
}

func (s *Store) addSnapJob(task func() error) (*util.Job, error) {
	return s.addNamedJob(snapWorker, task)
}

func (s *Store) addApplyJob(name string, task func() error) (*util.Job, error) {
	return s.addNamedJob(applyWorker, task)
}

func (s *Store) addSplitJob(task func() error) (*util.Job, error) {
	return s.addNamedJob(splitWorker, task)
}

func (s *Store) addNamedJob(worker string, task func() error) (*util.Job, error) {
	return s.runner.RunJobWithNamedWorker(worker, task)
}

func (s *Store) handleStoreHeartbeat() error {
	stats, err := util.DiskStats(s.cfg.StoreDataPath)
	if err != nil {
		return err
	}

	applySnapCount, err := s.getApplySnapshotCount()
	if err != nil {
		return err
	}

	req := new(pdpb.StoreHeartbeatReq)
	req.Header.ClusterID = s.clusterID
	req.Stats = &pdpb.StoreStats{
		StoreID:            s.GetID(),
		StartTime:          s.startAt,
		Capacity:           stats.Total,
		Available:          stats.Free,
		UsedSize:           stats.Used,
		CellCount:          s.replicatesMap.size(),
		SendingSnapCount:   s.sendingSnapCount,
		ReceivingSnapCount: s.reveivingSnapCount,
		ApplyingSnapCount:  applySnapCount,
		IsBusy:             false,
		BytesWritten:       0,
	}

	_, err = s.pdClient.StoreHeartbeat(context.TODO(), req)
	if err != nil {
		log.Errorf("heartbeat-store[%d]: heartbeat failed, errors:\n +%v",
			s.id,
			err)
	}

	return err
}

func (s *Store) getApplySnapshotCount() (uint32, error) {
	var cnt uint32
	err := s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if pr.ps.isApplyingSnap() {
			cnt++
		}
		return true, nil
	})

	return cnt, err
}

func (s *Store) handleCellHeartbeat() {
	for _, p := range s.replicatesMap.values() {
		p.checkPeers()
	}

	for _, p := range s.replicatesMap.values() {
		p.handleHeartbeat()
	}
}

func (s *Store) handleCellSplitCheck() {
	if s.runner.IsNamedWorkerBusy(splitWorker) {
		return
	}

	s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if !pr.isLeader() {
			return true, nil
		}

		if pr.sizeDiffHint < s.cfg.CellCheckSizeDiff {
			return true, nil
		}

		log.Infof("raftstore-split[cell-%d]: cell need to check whether should split, diff=<%d> max=<%d>",
			pr.cellID,
			pr.sizeDiffHint,
			s.cfg.CellCheckSizeDiff)

		err := pr.startSplitCheckJob()
		if err != nil {
			log.Errorf("raftstore-split[cell-%d]: add split check job failed, errors:\n %+v",
				pr.cellID,
				err)
			return false, err
		}

		pr.sizeDiffHint = 0
		return true, nil
	})
}

func (s *Store) handleRaftGCLog() {
	s.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		if !pr.isLeader() {
			return true, nil
		}

		// Leader will replicate the compact log command to followers,
		// If we use current replicated_index (like 10) as the compact index,
		// when we replicate this log, the newest replicated_index will be 11,
		// but we only compact the log to 10, not 11, at that time,
		// the first index is 10, and replicated_index is 11, with an extra log,
		// and we will do compact again with compact index 11, in cycles...
		// So we introduce a threshold, if replicated index - first index > threshold,
		// we will try to compact log.
		// raft log entries[..............................................]
		//                  ^                                       ^
		//                  |-----------------threshold------------ |
		//              first_index                         replicated_index

		var replicatedIdx uint64
		for _, p := range pr.rn.Status().Progress {
			if replicatedIdx == 0 {
				replicatedIdx = p.Match
			}

			if p.Match < replicatedIdx {
				replicatedIdx = p.Match
			}
		}

		// When an election happened or a new peer is added, replicated_idx can be 0.
		if replicatedIdx > 0 {
			lastIdx, _ := pr.ps.LastIndex()
			if lastIdx < replicatedIdx {
				log.Fatalf("raft-log-gc: expect last index >= replicated index, last=<%d> replicated=<%d>",
					lastIdx,
					replicatedIdx)
			}
		}

		var compactIdx uint64
		appliedIdx := pr.ps.getAppliedIndex()
		firstIdx, _ := pr.ps.FirstIndex()

		if appliedIdx >= firstIdx &&
			appliedIdx-firstIdx >= s.cfg.RaftLogGCCountLimit {
			compactIdx = appliedIdx
		} else if pr.sizeDiffHint >= s.cfg.RaftLogGCSizeLimit {
			compactIdx = appliedIdx
		} else if replicatedIdx < firstIdx ||
			replicatedIdx-firstIdx <= s.cfg.RaftLogGCThreshold {
			return true, nil
		}

		if compactIdx == 0 {
			compactIdx = replicatedIdx
		}

		// Have no idea why subtract 1 here, but original code did this by magic.
		if compactIdx == 0 {
			log.Fatal("raft-log-gc: unexpect compactIdx")
		}

		compactIdx--
		if compactIdx < firstIdx {
			// In case compactIdx == firstIdx before subtraction.
			return true, nil
		}

		term, _ := pr.ps.Term(compactIdx)

		req := newCompactLogRequest(compactIdx, term)
		s.sendAdminRequest(pr.getCell(), pr.peer, req)

		return true, nil
	})
}

func (s *Store) sendAdminRequest(cell metapb.Cell, peer metapb.Peer, adminReq *raftcmdpb.AdminRequest) {
	req := new(raftcmdpb.RaftCMDRequest)
	req.Header = &raftcmdpb.RaftRequestHeader{
		CellId:    cell.ID,
		CellEpoch: cell.Epoch,
		Peer:      peer,
		UUID:      uuid.NewV4().Bytes(),
	}
	req.AdminRequest = adminReq

	cmd := newCMD(req, nil)
	s.notify(cmd)
}

func (s *Store) getMetaEngine() storage.Engine {
	return s.engine.GetEngine()
}

func (s *Store) getDataEngine() storage.DataEngine {
	return s.engine.GetDataEngine()
}

func (s *Store) getKVEngine() storage.KVEngine {
	return s.engine.GetKVEngine()
}

func (s *Store) getHashEngine() storage.HashEngine {
	return s.engine.GetHashEngine()
}

func (s *Store) getListEngine() storage.ListEngine {
	return s.engine.GetListEngine()
}

func (s *Store) getSetEngine() storage.SetEngine {
	return s.engine.GetSetEngine()
}

func (s *Store) getZSetEngine() storage.ZSetEngine {
	return s.engine.GetZSetEngine()
}
