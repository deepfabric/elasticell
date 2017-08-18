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
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/etcd/raft"
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
	defaultChanSize       = 1024
)

type transport struct {
	store *Store

	server  *goetty.Server
	handler func(interface{})
	client  *pd.Client

	getStoreAddrFun func(storeID uint64) (string, error)

	conns map[string]goetty.IOSession
	msgs  chan *mraft.RaftMessage

	addrs   map[uint64]string
	ipAddrs map[string][]uint64

	readTimeout time.Duration
}

func newTransport(store *Store, client *pd.Client, handler func(interface{})) *transport {
	addr := store.cfg.StoreAddr
	if store.cfg.StoreAdvertiseAddr != "" {
		addr = store.cfg.StoreAdvertiseAddr
	}

	t := &transport{
		server:      goetty.NewServer(addr, decoder, encoder, goetty.NewUUIDV4IdGenerator()),
		conns:       make(map[string]goetty.IOSession),
		msgs:        make(chan *mraft.RaftMessage, defaultChanSize),
		addrs:       make(map[uint64]string),
		handler:     handler,
		client:      client,
		store:       store,
		readTimeout: time.Millisecond * time.Duration(store.cfg.Raft.BaseTick*store.cfg.Raft.ElectionTick),
	}

	t.getStoreAddrFun = t.getStoreAddr

	return t
}

func (t *transport) start() error {
	t.store.runner.RunCancelableTask(t.readyToSend)
	return t.server.Start(t.doConnection)
}

func (t *transport) stop() {
	t.server.Stop()
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

func (t *transport) send(msg *mraft.RaftMessage) {
	storeID := msg.ToPeer.StoreID

	if storeID == t.store.id {
		t.store.notify(msg)
		return
	}

	t.msgs <- msg
}

func (t *transport) readyToSend(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			close(t.msgs)
			return
		case msg := <-t.msgs:
			if msg != nil {
				err := t.doSend(msg)
				if err != nil {
					log.Errorf("raftstore[cell-%d]: send msg failure, from_peer=<%d> to_peer=<%d>, errors:\n%+v",
						msg.CellID,
						msg.FromPeer.ID,
						msg.ToPeer.ID,
						err)

					storeID := fmt.Sprintf("%d", msg.ToPeer.StoreID)

					pr := t.store.getPeerReplicate(msg.CellID)
					if pr != nil {
						raftFlowFailureReportCounterVec.WithLabelValues(labelRaftFlowFailureReportUnreachable, storeID).Inc()

						pr.rn.ReportUnreachable(msg.ToPeer.ID)
						if msg.Message.Type == raftpb.MsgSnap {
							pr.rn.ReportSnapshot(msg.ToPeer.ID, raft.SnapshotFailure)
							raftFlowFailureReportCounterVec.WithLabelValues(labelRaftFlowFailureReportSnapshot, storeID).Inc()
						}
					}
				}
			}
		}
	}
}

func (t *transport) doSend(msg *mraft.RaftMessage) error {
	storeID := msg.ToPeer.StoreID

	addr, err := t.getStoreAddrFun(storeID)
	if err != nil {
		return errors.Wrapf(err, "getStoreAddr")
	}

	conn, err := t.getConn(addr)
	if err != nil {
		return errors.Wrapf(err, "getConn")
	}

	// if we are send a snapshot raft msg, we can send sst files to the target store before.
	if msg.Message.Type == raftpb.MsgSnap {
		start := time.Now()

		snapData := &mraft.RaftSnapshotData{}
		util.MustUnmarshal(snapData, msg.Message.Snapshot.Data)

		if t.store.snapshotManager.Register(&snapData.Key, sending) {
			defer t.store.snapshotManager.Deregister(&snapData.Key, sending)

			if !t.store.snapshotManager.Exists(&snapData.Key) {
				return fmt.Errorf("transport: missing snapshot file, key=<%+v>",
					snapData.Key)
			}

			err = conn.Write(&snapData.Key)
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
			log.Debugf("transport: sent snapshot file complete, key=<%+v> size=<%d>",
				snapData.Key,
				size)
			observeSnapshotSending(start)
		}
	}

	err = conn.Write(msg)
	if err != nil {
		conn.Close()
		return errors.Wrapf(err, "write")
	}

	return nil
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

func (t *transport) getConn(addr string) (goetty.IOSession, error) {
	conn := t.getConnLocked(addr)
	if t.checkConnect(addr, conn) {
		return conn, nil
	}

	return conn, errConnect
}

func (t *transport) getConnLocked(addr string) goetty.IOSession {
	conn := t.conns[addr]

	if conn != nil {
		return conn
	}

	return t.createConn(addr)
}

func (t *transport) createConn(addr string) goetty.IOSession {
	conn := goetty.NewConnector(t.getConnectionCfg(addr), decoder, encoder)
	t.conns[addr] = conn
	return conn
}

func (t *transport) checkConnect(addr string, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Debugf("transport: connect to store failure, target=<%s> errors:\n %+v",
			addr,
			err)
		return false
	}

	log.Infof("transport: connected to store, target=<%s>", addr)
	return ok
}

func (t *transport) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
	}
}
