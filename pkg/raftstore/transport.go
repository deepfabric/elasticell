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
	"errors"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/fagongzi/goetty"
	"golang.org/x/net/context"
)

var (
	errConnect = errors.New("not connected")
)

var (
	tw = goetty.NewHashedTimeWheel(time.Millisecond*500, 60, 3)
)

const (
	defaultConnectTimeout = time.Second * 5
	defaultWriteIdle      = time.Second
)

type transport struct {
	sync.RWMutex

	server  *goetty.Server
	handler func(interface{})
	client  *pd.Client

	conns map[string]*goetty.Connector
	addrs map[uint64]string
}

func newTransport(cfg *RaftCfg, client *pd.Client, handler func(interface{})) *transport {
	addr := cfg.PeerAddr
	if cfg.PeerAdvertiseAddr != "" {
		addr = cfg.PeerAdvertiseAddr
	}

	return &transport{
		server:  goetty.NewServer(addr, decoder, encoder, goetty.NewUUIDV4IdGenerator()),
		conns:   make(map[string]*goetty.Connector),
		addrs:   make(map[uint64]string),
		handler: handler,
		client:  client,
	}
}

func (t *transport) start() error {
	return t.server.Start(t.doConnection)
}

func (t *transport) stop() {
	t.server.Stop()
}

func (t *transport) doConnection(session goetty.IOSession) error {
	for {
		msg, err := session.Read()
		if err != nil {
			return err
		}

		t.handler(msg)
		return nil
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
	addr, err := t.getStoreAddr(storeID)
	if err != nil {
		return err
	}

	conn, err := t.getConn(addr)
	if err != nil {
		return err
	}

	return conn.Write(msg)
}

func (t *transport) getConn(addr string) (*goetty.Connector, error) {
	conn := t.getConnLocked(addr)
	if checkConnect(addr, conn) {
		return conn, nil
	}

	return conn, errConnect
}

func (t *transport) getConnLocked(addr string) *goetty.Connector {
	t.RLock()
	conn := t.conns[addr]
	t.RUnlock()

	if conn != nil {
		return conn
	}

	return t.createConn(addr)
}

func (t *transport) createConn(addr string) *goetty.Connector {
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

func checkConnect(addr string, conn *goetty.Connector) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("transport: connect to store failure, target=<%s> errors:\n %+v",
			addr,
			err)
		return false
	}

	return ok
}

func (t *transport) getConnectionCfg(addr string) *goetty.Conf {
	return &goetty.Conf{
		Addr: addr,
		TimeoutConnectToServer: defaultConnectTimeout,
		TimeWheel:              tw,
		TimeoutWrite:           defaultWriteIdle,
		WriteTimeoutFn:         t.onTimeIdle,
	}
}

func (t *transport) onTimeIdle(addr string, conn *goetty.Connector) {

}
