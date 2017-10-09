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
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
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
	defaultConnectTimeout = time.Second * 5
	defaultWriteIdle      = time.Second * 30
)

type transport struct {
	sync.RWMutex

	store *Store

	server  *goetty.Server
	handler func(interface{})
	client  *pd.Client

	getStoreAddrFun func(storeID uint64) (string, error)

	conns     map[uint64]goetty.IOSessionPool
	msgs      []*util.Queue
	snapshots *util.Queue
	mask      uint64

	addrs   map[uint64]string
	ipAddrs map[string][]uint64

	timeWheel        *goetty.TimeoutWheel
	readTimeout      time.Duration
	heartbeatTimeout time.Duration
}

func newTransport(store *Store, client *pd.Client, handler func(interface{})) *transport {
	addr := globalCfg.StoreAddr
	if globalCfg.StoreAdvertiseAddr != "" {
		addr = globalCfg.StoreAdvertiseAddr
	}

	t := &transport{
		server:           goetty.NewServer(addr, decoder, encoder, goetty.NewUUIDV4IdGenerator()),
		conns:            make(map[uint64]goetty.IOSessionPool),
		addrs:            make(map[uint64]string),
		msgs:             make([]*util.Queue, globalCfg.RaftMessageWorkerCount, globalCfg.RaftMessageWorkerCount),
		mask:             globalCfg.RaftMessageWorkerCount - 1,
		handler:          handler,
		client:           client,
		store:            store,
		snapshots:        &util.Queue{},
		readTimeout:      time.Millisecond * time.Duration(globalCfg.Raft.BaseTick*globalCfg.Raft.ElectionTick) * 10,
		heartbeatTimeout: time.Millisecond * time.Duration(globalCfg.Raft.BaseTick*globalCfg.Raft.ElectionTick),
		timeWheel:        goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Second)),
	}

	t.getStoreAddrFun = t.getStoreAddr

	return t
}

func (t *transport) start() error {
	for i := uint64(0); i < globalCfg.RaftMessageWorkerCount; i++ {
		t.msgs[i] = &util.Queue{}
		go t.readyToSend(t.msgs[i])
	}

	go t.readToSendSnapshots()

	return t.server.Start(t.doConnection)
}

func (t *transport) stop() {
	t.snapshots.Dispose()

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
		msg, err := session.ReadTimeout(t.readTimeout)
		if err != nil {
			if err == io.EOF {
				log.Infof("transport: closed by %s", remoteIP)
			} else {
				log.Warnf("transport: read error from conn-%s, errors:\n%+v", remoteIP, err)
			}

			return err
		}

		t.handler(msg)
	}
}

func (t *transport) send(msg *mraft.RaftMessage) error {
	storeID := msg.ToPeer.StoreID

	if storeID == t.store.id {
		t.store.notify(msg)
		return nil
	}

	if msg.Message.Type == raftpb.MsgSnap {
		m := &mraft.RaftMessage{}
		util.MustUnmarshal(m, util.MustMarshal(msg))
		err := t.snapshots.Put(m)
		if err != nil {
			return err
		}
	}

	return t.msgs[t.mask&storeID].Put(msg)
}

func (t *transport) readyToSend(q *util.Queue) {
	items := make([]interface{}, globalCfg.RaftMessageSendBatchLimit, globalCfg.RaftMessageSendBatchLimit)

	for {
		n, err := q.Get(globalCfg.RaftMessageSendBatchLimit, items)

		if err != nil {
			log.Infof("stop: raft transfer send worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*mraft.RaftMessage)
			err := t.doSendRaftMessage(msg)
			t.postSend(msg, err)
		}

		queueGauge.WithLabelValues(labelQueueMsgs).Set(float64(q.Len()))
	}
}

func (t *transport) readToSendSnapshots() {
	snapConns := make(map[uint64]goetty.IOSession)
	items := make([]interface{}, globalCfg.RaftMessageSendBatchLimit, globalCfg.RaftMessageSendBatchLimit)
	for {
		n, err := t.snapshots.Get(globalCfg.RaftMessageSendBatchLimit, items)
		if err != nil {
			log.Infof("stop: raft transfer send snapshot worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			var id uint64
			var msg *mraft.RaftMessage
			var hb *heartbeatMsg

			if m, ok := items[i].(uint64); ok {
				id = m
				hb = heartbeat
			} else if m, ok := items[i].(*mraft.RaftMessage); ok {
				id = m.ToPeer.StoreID
				msg = m
			}

			conn := snapConns[id]
			if conn == nil {
				conn, err = t.createConn(id)
				if err != nil {
					log.Debugf("transport: create conn to %d failed, errors:\n%+v",
						id,
						err)
					continue
				}

				snapConns[id] = conn
			}

			if !conn.IsConnected() {
				_, err := conn.Connect()
				if err != nil {
					log.Debugf("transport: connect to %d failed, errors:\n%+v",
						id,
						err)
					continue
				}
			}

			if hb != nil {
				conn.Write(heartbeat)
			} else {
				t.doSendSnap(msg, conn)
			}
		}

		queueGauge.WithLabelValues(labelQueueSnaps).Set(float64(t.snapshots.Len()))
	}
}

func (t *transport) doSendRaftMessage(msg *mraft.RaftMessage) error {
	conn, err := t.getConn(msg.ToPeer.StoreID)
	if err != nil {
		return errors.Wrapf(err, "getConn")
	}

	err = conn.Write(msg)
	if err != nil {
		conn.Close()
		err = errors.Wrapf(err, "write")
	}

	t.putConn(msg.ToPeer.StoreID, conn)
	return err
}

func (t *transport) doSendSnap(msg *mraft.RaftMessage, conn goetty.IOSession) error {
	snapData := &mraft.RaftSnapshotData{}
	util.MustUnmarshal(snapData, msg.Message.Snapshot.Data)

	if t.store.snapshotManager.Register(&snapData.Key, sending) {
		defer t.store.snapshotManager.Deregister(&snapData.Key, sending)

		start := time.Now()
		if !t.store.snapshotManager.Exists(&snapData.Key) {
			return fmt.Errorf("transport: missing snapshot file, key=<%+v>",
				snapData.Key)
		}

		err := conn.Write(&snapData.Key)
		if err != nil {
			conn.Close()
			return errors.Wrapf(err, "")
		}

		size, err := t.store.snapshotManager.WriteTo(&snapData.Key, conn)
		if err != nil {
			conn.Close()
			return errors.Wrapf(err, "")
		}

		if snapData.FileSize != size {
			return fmt.Errorf("transport: snapshot file size not match, size=<%d> sent=<%d>",
				snapData.FileSize, size)
		}

		err = conn.Write(&mraft.SnapshotDataEnd{
			Key:      snapData.Key,
			FileSize: snapData.FileSize,
			CheckSum: snapData.CheckSum,
		})
		if err != nil {
			conn.Close()
			return errors.Wrapf(err, "")
		}

		t.store.sendingSnapCount++
		log.Infof("transport: sent snapshot file complete, key=<%+v> size=<%d>",
			snapData.Key,
			size)
		observeSnapshotSending(start)

		t.store.addSnapJob(func() error {
			return t.store.snapshotManager.CleanSnap(&snapData.Key)
		}, nil)
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
	} else {
		if msg.Message.Type == raftpb.MsgSnap {
			pr := t.store.getPeerReplicate(msg.CellID)
			if pr != nil {
				pr.report(msg.ToPeer.ID)
			}
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
			pool, err = goetty.NewIOSessionPool(1, 1, func() (goetty.IOSession, error) {
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

	conn := goetty.NewConnector(t.getConnectionCfg(addr), decoder, encoder)
	conn.SetAttr("store", id)

	_, err = conn.Connect()
	return conn, err
}

func (t *transport) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
		TimeWheel:              t.timeWheel,
		TimeoutWrite:           t.heartbeatTimeout,
		WriteTimeoutFn:         t.sendHeartbeat,
	}
}

func (t *transport) sendHeartbeat(addr string, conn goetty.IOSession) {
	if t.snapshots.Len() == 0 {
		t.snapshots.Put(conn.GetAttr("store"))
	}
}
