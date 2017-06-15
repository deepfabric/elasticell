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
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	. "github.com/pingcap/check"
)

type testTransportSuite struct {
	trans *transport
}

func (s *testTransportSuite) SetUpSuite(c *C) {
	cfg := newTestStoreCfg(1)
	store := new(Store)
	store.cfg = cfg

	s.trans = newTransport(store, nil, nil)
	go s.trans.start()
	<-s.trans.server.Started()
}

func (s *testTransportSuite) TearDownSuite(c *C) {
	s.trans.stop()
}

func (s *testTransportSuite) TestSendRaftMsg(c *C) {
	complete := make(chan struct{}, 1)
	cnt := 0
	s.trans.handler = func(msg interface{}) {
		cnt++
		complete <- struct{}{}
	}

	s.trans.getStoreAddrFun = func(storeID uint64) (string, error) {
		return s.trans.store.cfg.StoreAddr, nil
	}

	msg := new(mraft.RaftMessage)
	err := s.trans.send(1, msg)
	c.Assert(err, IsNil)

	select {
	case <-complete:
		c.Assert(cnt, Equals, 1)
	case <-time.After(time.Second):
		c.Fatal("send raft msg timeout")
	}
}
