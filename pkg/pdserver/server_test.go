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
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	. "github.com/pingcap/check"
)

var _testSingleSvr *Server

func startTestSingleServer() {
	_testSingleSvr = NewTestSingleServer()
	_testSingleSvr.Start()
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

func (s *testServerSuite) stopMultiPDServers(c *C) {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}

	if s.servers != nil {
		for _, s := range s.servers {
			s.Stop()
			os.RemoveAll(s.GetCfg().DataPath)
		}

		s.servers = nil
	}
}

func (s *testServerSuite) restartMultiPDServer(c *C, count int) {
	s.stopMultiPDServers(c)
	file, _ := ioutil.TempDir("", "etcd-log")
	f, err := os.Open(file)
	c.Assert(err, IsNil)
	RedirectEmbedEtcdLog(f)

	s.servers = NewTestMultiServers(count)
	var addrs []string
	var wg sync.WaitGroup
	for index := 0; index < count; index++ {
		addrs = append(addrs, s.servers[index].cfg.AddrRPC)
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			s.servers[index].Start()
		}(index)
	}

	wg.Wait()

	s.client, err = pd.NewClient("test-pd-cli", addrs...)
	c.Assert(err, IsNil)
}

func (s *testServerSuite) SetUpSuite(c *C) {

}

func (s *testServerSuite) TearDownSuite(c *C) {
	s.stopMultiPDServers(c)
}

func (s *testServerSuite) bootstrapCluster() (*pdpb.BootstrapClusterRsp, error) {
	rsp, err := s.client.AllocID(context.TODO(), &pdpb.AllocIDReq{})
	if err != nil {
		return nil, err
	}

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
	if err != nil {
		return nil, err
	}

	peer := metapb.Peer{
		ID:      rsp.ID,
		StoreID: store.ID,
	}

	rsp, err = s.client.AllocID(context.TODO(), &pdpb.AllocIDReq{})
	if err != nil {
		return nil, err
	}

	var cells []metapb.Cell
	cells = append(cells, metapb.Cell{
		ID:    rsp.ID,
		Peers: []*metapb.Peer{&peer},
	})

	return s.client.BootstrapCluster(context.TODO(), &pdpb.BootstrapClusterReq{
		Store: store,
		Cells: cells,
	})
}

func (s *testServerSuite) TestServerBootstrap(c *C) {
	s.restartMultiPDServer(c, 3)
	rsp, err := s.bootstrapCluster()

	c.Assert(err, IsNil)
	c.Assert(rsp.AlreadyBootstrapped, IsFalse)

	rsp, err = s.bootstrapCluster()
	c.Assert(err, IsNil)
	c.Assert(rsp.AlreadyBootstrapped, IsTrue)
}
