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
	"sync"

	"errors"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/fagongzi/goetty"
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
	handler func(msg *mraft.RaftMessage)

	conns map[string]*goetty.Connector
}

func newTransport(cfg *RaftCfg, handler func(msg *mraft.RaftMessage)) *transport {
	addr := cfg.PeerAddr
	if cfg.PeerAdvertiseAddr != "" {
		addr = cfg.PeerAdvertiseAddr
	}
	
	return &transport{
		server:  goetty.NewServer(addr, decoder, encoder, goetty.NewUUIDV4IdGenerator()),
		conns:   make(map[string]*goetty.Connector, 32),
		handler: handler,
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
		origin, err := session.Read()
		if err != nil {
			return err
		}

		msg := origin.(*mraft.RaftMessage)
		t.handler(msg)
		return nil
	}
}

func (t *transport) send(addr string, msg *mraft.RaftMessage) error {
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
