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

package pdserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	. "github.com/pingcap/check"
)

var _testSingleSvr *Server

func startTestSingleServer() {
	_testSingleSvr = newTestSingleServer()
	_testSingleSvr.Start()
	time.Sleep(time.Second * 1)
}

func stopTestSingleServer() {
	if _testSingleSvr != nil {
		_testSingleSvr.Stop()
	}
}

func TestServer(t *testing.T) {
	startTestSingleServer()
	defer stopTestSingleServer()
	TestingT(t)
}

var _ = Suite(&testServerSuite{})

type testServerSuite struct {
	count   int
	servers []*Server
	client  *pd.Client
}

func (s *testServerSuite) SetUpSuite(c *C) {
	s.count = 3
	s.servers = newTestMultiServers(s.count)

	var addrs []string
	var wg sync.WaitGroup
	for index := 0; index < s.count; index++ {
		addrs = append(addrs, s.servers[index].cfg.RPCAddr)
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			s.servers[index].Start()
		}(index)
	}

	wg.Wait()
	time.Sleep(time.Second * 5)

	var err error
	s.client, err = pd.NewClient("test-pd-cli", addrs...)
	c.Assert(err, IsNil)
}

func (s *testServerSuite) TearDownSuite(c *C) {
	if s.client != nil {
		s.client.Close()
	}

	var wg sync.WaitGroup
	for index := 0; index < s.count; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			s.servers[index].Stop()
		}(index)
	}
	wg.Wait()
}

func (s *testServerSuite) TestServerLeaderCount(c *C) {
	leaderCount := 0

	for _, svr := range s.servers {
		if svr.IsLeader() {
			leaderCount++
		}
	}

	c.Assert(leaderCount, Equals, 1)
}

func (s *testServerSuite) TestServerBootstrap(c *C) {
	rsp, err := s.client.AllocID(context.TODO(), &pdpb.AllocIDReq{})
	c.Assert(err, IsNil)

	var lables []metapb.Label
	lables = append(lables, metapb.Label{
		Key:   "rack",
		Value: "001",
	})

	store := metapb.Store{
		ID:      rsp.ID,
		Address: ":6379",
		Lables:  lables,
		State:   metapb.UP,
	}

	rsp, err = s.client.AllocID(context.TODO(), &pdpb.AllocIDReq{})
	c.Assert(err, IsNil)
	peer := metapb.Peer{
		ID:      rsp.ID,
		StoreID: store.ID,
	}

	rsp, err = s.client.AllocID(context.TODO(), &pdpb.AllocIDReq{})
	c.Assert(err, IsNil)
	cell := metapb.Cell{
		ID:    rsp.ID,
		Peers: []*metapb.Peer{&peer},
	}

	brsp, err := s.client.BootstrapCluster(context.TODO(), &pdpb.BootstrapClusterReq{
		Store: store,
		Cell:  cell,
	})
	c.Assert(err, IsNil)
	c.Assert(brsp.AlreadyBootstrapped, IsFalse)
}
