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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/querypb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/etcd/raft/raftpb"
	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var (
	errConnect = errors.New("not connected")
)

const (
	defaultConnectTimeout = time.Second * 10
)

type transport struct {
	sync.RWMutex

	store *Store

	seq uint64

	server  *goetty.Server
	handler func(interface{})
	client  *pd.Client

	getStoreAddrFun func(storeID uint64) (string, error)

	conns     map[uint64]goetty.IOSessionPool
	msgs      []*util.Queue
	snapshots []*util.Queue
	mask      uint64
	snapMask  uint64

	waitACKs         map[uint64]*mraft.SnapshotMessage
	sendingSnapshots map[uint64]*mraft.RaftMessage
	pendingLock      sync.RWMutex

	addrs       map[uint64]string
	addrsRevert map[string]uint64
}

func newTransport(store *Store, client *pd.Client, handler func(interface{})) *transport {
	t := &transport{
		server:           goetty.NewServer(globalCfg.Addr, decoder, encoder, goetty.NewUUIDV4IdGenerator()),
		conns:            make(map[uint64]goetty.IOSessionPool),
		addrs:            make(map[uint64]string),
		addrsRevert:      make(map[string]uint64),
		msgs:             make([]*util.Queue, globalCfg.WorkerCountSent, globalCfg.WorkerCountSent),
		snapshots:        make([]*util.Queue, globalCfg.WorkerCountSentSnap, globalCfg.WorkerCountSentSnap),
		mask:             globalCfg.WorkerCountSent - 1,
		snapMask:         globalCfg.WorkerCountSentSnap - 1,
		sendingSnapshots: make(map[uint64]*mraft.RaftMessage),
		waitACKs:         make(map[uint64]*mraft.SnapshotMessage),
		handler:          handler,
		client:           client,
		store:            store,
	}

	t.getStoreAddrFun = t.getStoreAddr

	return t
}

func (t *transport) start() error {
	for i := uint64(0); i < globalCfg.WorkerCountSent; i++ {
		t.msgs[i] = &util.Queue{}
		go t.readyToSendRaft(t.msgs[i])
	}

	for i := uint64(0); i < globalCfg.WorkerCountSentSnap; i++ {
		t.snapshots[i] = &util.Queue{}
		go t.readyToSendSnapshots(t.snapshots[i])
	}

	return t.server.Start(t.doConnection)
}

func (t *transport) stop() {
	for _, q := range t.snapshots {
		q.Dispose()
	}

	for _, q := range t.msgs {
		q.Dispose()
	}

	t.server.Stop()
	log.Infof("stopped: transfer stopped")
}

func (t *transport) doConnection(session goetty.IOSession) error {
	remoteIP := session.RemoteIP()

	log.Infof("transport: %s connected", remoteIP)
	for {
		msg, err := session.Read()
		if err != nil {
			if err == io.EOF {
				log.Infof("transport: closed by %s", remoteIP)
			} else {
				log.Warnf("transport: read error from conn-%s, errors:\n%+v", remoteIP, err)
			}

			return err
		}

		if v, ok := msg.(*mraft.SnapshotMessage); ok {
			t.sendACK(v)
		} else if v, ok := msg.(*mraft.ACKMessage); ok {
			t.removeACK(v.Seq)
			continue
		}

		t.handler(msg)
	}
}

func (t *transport) sendSnapshotMessage(msg *mraft.SnapshotMessage) {
	msg.Header.Seq = t.nexSeq()
	t.snapshots[t.snapMask&msg.Header.ToPeer.StoreID].Put(msg)
}

func (t *transport) sendACK(msg *mraft.SnapshotMessage) {
	ack := &mraft.ACKMessage{
		Seq: msg.Header.Seq,
	}

	t.doSend(ack, msg.Header.FromPeer.StoreID)
}

func (t *transport) sendQuery(msg interface{}) {
	var storeID uint64
	switch msg2 := msg.(type) {
	case *querypb.QueryReq:
		storeID = msg2.ToStore
	case *querypb.QueryRsp:
		storeID = msg2.ToStore
	}

	if storeID == t.store.id {
		t.store.notify(msg)
		return
	}
	t.msgs[t.mask&storeID].Put(msg)
}

func (t *transport) sendRaftMessage(msg *mraft.RaftMessage) {
	storeID := msg.ToPeer.StoreID

	if storeID == t.store.id {
		t.store.notify(msg)
		return
	}

	if msg.Message.Type == raftpb.MsgSnap {
		err := t.addPendingSnapshot(msg)
		if err != nil {
			t.postSend(msg, err)
			return
		}

		snapMsg := &mraft.SnapshotMessage{}
		util.MustUnmarshal(snapMsg, msg.Message.Snapshot.Data)
		snapMsg.Header.FromPeer = msg.FromPeer
		snapMsg.Header.ToPeer = msg.ToPeer
		snapMsg.Ask = &mraft.SnapshotAskMessage{}

		t.snapshots[t.snapMask&storeID].Put(snapMsg)
	}

	t.msgs[t.mask&storeID].Put(msg)
}

func (t *transport) readyToSendRaft(q *util.Queue) {
	items := make([]interface{}, globalCfg.BatchSizeSent, globalCfg.BatchSizeSent)

	for {
		n, err := q.Get(int64(globalCfg.BatchSizeSent), items)
		if err != nil {
			log.Infof("stop: raft transfer send worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			switch msg := items[i].(type) {
			case *mraft.RaftMessage:
				err := t.doSend(msg, msg.ToPeer.StoreID)
				t.postSend(msg, err)
			case *querypb.QueryReq, *querypb.QueryRsp:
				t.doSendQuery(items[i])
			}
		}

		queueGauge.WithLabelValues(labelQueueMsgs).Set(float64(q.Len()))
	}
}

func (t *transport) readyToSendSnapshots(q *util.Queue) {
	items := make([]interface{}, globalCfg.BatchSizeSent, globalCfg.BatchSizeSent)

	for {
		n, err := q.Get(int64(globalCfg.BatchSizeSent), items)
		if err != nil {
			log.Infof("stop: raft transfer send snapshot worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*mraft.SnapshotMessage)
			id := msg.Header.ToPeer.StoreID

			if (isOnlyHeader(msg) || msg.Ask != nil) &&
				nil == t.getSendingSnapshot(msg.Header.ToPeer.ID) {
				continue
			}

			conn, err := t.getConn(id)
			if err != nil {
				log.Errorf("transport: create conn to %d failed, errors:\n%v",
					id,
					err)
				t.retrySnapshot(msg)
				continue
			}

			err = t.doSendSnapshotMessage(msg, conn)
			t.putConn(id, conn)

			if err != nil {
				log.Errorf("transport: send snap failed, snap=<%s>,errors:\n%v",
					msg.String(),
					err)
				t.retrySnapshot(msg)
				continue
			}

			// If we write succ, wait ack messages,
			// Otherwise, resent if timeout
			t.waitACK(msg)
		}

		queueGauge.WithLabelValues(labelQueueSnaps).Set(float64(q.Len()))
	}
}

func (t *transport) doSendQuery(item interface{}) {
	var toStore, fromStore uint64
	switch msg := item.(type) {
	case *querypb.QueryReq:
		toStore = msg.ToStore
		fromStore = msg.FromStore
	case *querypb.QueryRsp:
		toStore = msg.ToStore
		fromStore = msg.FromStore
	}
	conn, err := t.getConn(toStore)
	if err != nil {
		log.Errorf("raftstore: doSendQuery failed, from_store=<%d> to_store=<%d>, errors:\n%s", fromStore, toStore, err)
		return
	}
	err = conn.Write(item)
	if err != nil {
		conn.Close()
		log.Errorf("raftstore: doSendQuery failed, from_store=<%d> to_store=<%d>, errors:\n%s", fromStore, toStore, err)
		return
	}

	t.putConn(toStore, conn)
	return
}

func (t *transport) waitACK(msg *mraft.SnapshotMessage) {
	t.pendingLock.Lock()
	t.waitACKs[msg.Header.Seq] = msg
	t.pendingLock.Unlock()

	util.DefaultTimeoutWheel().Schedule(globalCfg.DurationRetrySentSnapshot, t.doWaitTimeoutSnapshot, msg)
}

func (t *transport) removeACK(id uint64) {
	t.pendingLock.Lock()
	delete(t.waitACKs, id)
	t.pendingLock.Unlock()
}

func (t *transport) isWaitting(id uint64) bool {
	t.pendingLock.RLock()
	_, ok := t.waitACKs[id]
	t.pendingLock.RUnlock()
	return ok
}

func (t *transport) retrySnapshot(msg *mraft.SnapshotMessage) {
	util.DefaultTimeoutWheel().Schedule(globalCfg.DurationRetrySentSnapshot, t.doRetrySnapshot, msg)
}

func (t *transport) addPendingSnapshot(msg *mraft.RaftMessage) error {
	t.pendingLock.Lock()
	check, ok := t.sendingSnapshots[msg.ToPeer.ID]
	if !ok || (!StalEpoch(msg.CellEpoch, check.CellEpoch) &&
		check.Message.Snapshot.Metadata.Term < msg.Message.Snapshot.Metadata.Term &&
		check.Message.Snapshot.Metadata.Index < msg.Message.Snapshot.Metadata.Index) {

		snap := &mraft.RaftMessage{}
		util.MustUnmarshal(snap, util.MustMarshal(msg))
		t.sendingSnapshots[msg.ToPeer.ID] = snap
		log.Infof("raftstore-snap[cell-%d]: pending snap added, epoch=<%s> term=<%d> index=<%d>",
			msg.CellID,
			msg.CellEpoch.String(),
			msg.Message.Snapshot.Metadata.Term,
			msg.Message.Snapshot.Metadata.Index)
		t.pendingLock.Unlock()
		return nil
	}

	t.pendingLock.Unlock()
	return fmt.Errorf("stale snapshot, sending=<%s>, add=<%s>",
		check.String(),
		msg.CellEpoch.String())
}

func (t *transport) getSendingSnapshot(peerID uint64) *mraft.RaftMessage {
	t.pendingLock.RLock()
	value := t.sendingSnapshots[peerID]
	t.pendingLock.RUnlock()

	return value
}

// remove sending snapshots
// it must called only if peer will removed
func (t *transport) forceRemoveSendingSnapshot(peerID uint64) {
	t.pendingLock.Lock()
	delete(t.sendingSnapshots, peerID)
	t.pendingLock.Unlock()
}

// remove sending snapshots, it will skip to retry if not received ack by peer
func (t *transport) removeSendingSnapshot(peerID uint64, head mraft.SnapshotMessageHeader) {
	t.pendingLock.Lock()
	if check, ok := t.sendingSnapshots[peerID]; ok &&
		head.Cell.Epoch.CellVer == check.CellEpoch.CellVer &&
		head.Cell.Epoch.ConfVer == check.CellEpoch.ConfVer &&
		head.Term == check.Message.Snapshot.Metadata.Term &&
		head.Index == check.Message.Snapshot.Metadata.Index {
		delete(t.sendingSnapshots, peerID)
		log.Infof("raftstore-snap[cell-%d]: pending snap removed, epoch=<%s> term=<%d> index=<%d>",
			check.CellID,
			check.CellEpoch.String(),
			check.Message.Snapshot.Metadata.Term,
			check.Message.Snapshot.Metadata.Index)
	}
	t.pendingLock.Unlock()
}

func (t *transport) doRetrySnapshot(arg interface{}) {
	msg := arg.(*mraft.SnapshotMessage)
	t.snapshots[t.snapMask&msg.Header.ToPeer.StoreID].Put(msg)
}

func (t *transport) doWaitTimeoutSnapshot(arg interface{}) {
	msg := arg.(*mraft.SnapshotMessage)
	if t.isWaitting(msg.Header.Seq) {
		t.snapshots[t.snapMask&msg.Header.ToPeer.StoreID].Put(msg)
	}
}

func (t *transport) doSend(msg interface{}, to uint64) error {
	conn, err := t.getConn(to)
	if err != nil {
		return errors.Wrapf(err, "getConn")
	}

	err = t.doWrite(msg, conn)
	t.putConn(to, conn)

	return err
}

func (t *transport) doWrite(msg interface{}, conn goetty.IOSession) error {
	err := conn.Write(msg)
	if err != nil {
		conn.Close()
		err = errors.Wrapf(err, "write")
	}

	return nil
}

func (t *transport) doSendSnapshotMessage(msg *mraft.SnapshotMessage, conn goetty.IOSession) error {
	if !isOnlyHeader(msg) {
		return t.doWrite(msg, conn)
	}

	raftMessage := t.getSendingSnapshot(msg.Header.ToPeer.ID)
	if nil == raftMessage {
		log.Fatalf("bug: sending snapshot can not be nil")
	}

	if t.store.snapshotManager.Register(msg, sending) {
		defer t.store.snapshotManager.Deregister(msg, sending)

		log.Infof("raftstore-snap[cell-%d]: start send pending snap, epoch=<%s> term=<%d> index=<%d>",
			msg.Header.Cell.ID,
			msg.Header.Cell.Epoch.String(),
			msg.Header.Term,
			msg.Header.Index)

		start := time.Now()
		if !t.store.snapshotManager.Exists(msg) {
			return fmt.Errorf("transport: missing snapshot file, header=<%+v>",
				msg.Header)
		}

		size, err := t.store.snapshotManager.WriteTo(msg, conn)
		if err != nil {
			conn.Close()
			return errors.Wrapf(err, "")
		}

		err = t.doWrite(raftMessage, conn)
		if err != nil {
			return err
		}

		t.store.sendingSnapCount++
		t.removeSendingSnapshot(msg.Header.ToPeer.ID, msg.Header)
		log.Infof("raftstore-snap[cell-%d]: pending snap sent succ, size=<%d>, epoch=<%s> term=<%d> index=<%d>",
			msg.Header.Cell.ID,
			size,
			msg.Header.Cell.Epoch.String(),
			msg.Header.Term,
			msg.Header.Index)

		observeSnapshotSending(start)
	}

	return nil
}

func (t *transport) postSend(msg *mraft.RaftMessage, err error) {
	if err != nil {
		log.Errorf("raftstore[cell-%d]: send msg failure, from_peer=<%d> to_peer=<%d>, errors:\n%s",
			msg.CellID,
			msg.FromPeer.ID,
			msg.ToPeer.ID,
			err)

		storeID := fmt.Sprintf("%d", msg.ToPeer.StoreID)

		pr := t.store.getPeerReplicate(msg.CellID)
		if pr != nil {
			raftFlowFailureReportCounterVec.WithLabelValues(labelRaftFlowFailureReportUnreachable, storeID).Inc()
			if msg.Message.Type == raftpb.MsgSnap {
				raftFlowFailureReportCounterVec.WithLabelValues(labelRaftFlowFailureReportSnapshot, storeID).Inc()
			}

			pr.report(msg.Message)
		}
	}

	pool.ReleaseRaftMessage(msg)
}

func (t *transport) getStoreAddr(storeID uint64) (string, error) {
	addr, ok := t.addrs[storeID]

	if !ok {
		addr, ok = t.addrs[storeID]
		if ok {
			return addr, nil
		}

		rsp, err := t.client.GetStore(context.TODO(), &pdpb.GetStoreReq{
			StoreID: storeID,
		})

		if err != nil {
			return "", err
		}

		addr = rsp.Store.Address
		t.addrs[storeID] = addr
		t.addrsRevert[addr] = storeID
	}

	return addr, nil
}

func (t *transport) putConn(id uint64, conn goetty.IOSession) {
	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool != nil {
		pool.Put(conn)
	} else {
		conn.Close()
	}
}

func (t *transport) getConn(storeID uint64) (goetty.IOSession, error) {
	conn, err := t.getConnLocked(storeID)
	if err != nil {
		return nil, err
	}

	if t.checkConnect(storeID, conn) {
		return conn, nil
	}

	t.putConn(storeID, conn)
	return nil, errConnect
}

func (t *transport) getConnLocked(id uint64) (goetty.IOSession, error) {
	var err error

	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool == nil {
		t.Lock()

		pool = t.conns[id]
		if pool == nil {
			pool, err = goetty.NewIOSessionPool(1, 2, func() (goetty.IOSession, error) {
				return t.createConn(id)
			})

			if err != nil {
				return nil, err
			}

			t.conns[id] = pool
		}
		t.Unlock()
	}

	return pool.Get()
}

func (t *transport) checkConnect(id uint64, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("transport: connect to store failure, target=<%d> errors:\n %+v",
			id,
			err)
		return false
	}

	log.Infof("transport: connected to store, target=<%d>", id)
	return ok
}

func (t *transport) createConn(id uint64) (goetty.IOSession, error) {
	addr, err := t.getStoreAddrFun(id)
	if err != nil {
		return nil, errors.Wrapf(err, "getStoreAddr")
	}

	return goetty.NewConnector(t.getConnectionCfg(addr), decoder, encoder), nil
}

func (t *transport) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
	}
}

func (t *transport) nexSeq() uint64 {
	return atomic.AddUint64(&t.seq, 1)
}

func isOnlyHeader(msg *mraft.SnapshotMessage) bool {
	return msg.Ack == nil && msg.Chunk == nil && msg.Ask == nil
}
