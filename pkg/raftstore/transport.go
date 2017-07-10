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

	conns   map[string]goetty.IOSession
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

func (t *transport) getStoreAddr(storeID uint64) (string, error) {
	t.RLock()
	addr, ok := t.addrs[storeID]
	t.RUnlock()

	if !ok {
		t.Lock()
		addr, ok = t.addrs[storeID]
		if ok {
			t.Unlock()
			return addr, nil
		}

		rsp, err := t.client.GetStore(context.TODO(), &pdpb.GetStoreReq{
			StoreID: storeID,
		})

		if err != nil {
			t.Unlock()
			return "", err
		}

		addr = rsp.Store.Address
		t.addrs[storeID] = addr
		t.Unlock()
	}

	return addr, nil
}

func (t *transport) send(storeID uint64, msg *mraft.RaftMessage) error {
	if storeID == t.store.id {
		t.store.notify(msg)
		return nil
	}

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
		}
	}

	err = conn.Write(msg)
	if err != nil {
		conn.Close()
		return errors.Wrapf(err, "write")
	}

	return nil
}

func (t *transport) getConn(addr string) (goetty.IOSession, error) {
	conn := t.getConnLocked(addr)
	if t.checkConnect(addr, conn) {
		return conn, nil
	}

	return conn, errConnect
}

func (t *transport) getConnLocked(addr string) goetty.IOSession {
	t.RLock()
	conn := t.conns[addr]
	t.RUnlock()

	if conn != nil {
		return conn
	}

	return t.createConn(addr)
}

func (t *transport) createConn(addr string) goetty.IOSession {
	t.Lock()

	// double check
	if conn, ok := t.conns[addr]; ok {
		t.Unlock()
		return conn
	}

	conn := goetty.NewConnector(t.getConnectionCfg(addr), decoder, encoder)
	t.conns[addr] = conn
	t.Unlock()
	return conn
}

func (t *transport) checkConnect(addr string, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	t.Lock()
	if conn.IsConnected() {
		t.Unlock()
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("transport: connect to store failure, target=<%s> errors:\n %+v",
			addr,
			err)
		t.Unlock()
		return false
	}

	log.Infof("transport: connected to store, target=<%s>", addr)
	t.Unlock()
	return ok
}

func (t *transport) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
	}
}
