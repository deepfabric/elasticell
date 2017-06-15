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
	"io/ioutil"
	"sync"
	"time"

	"fmt"

	"os"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/pdserver"
	"github.com/deepfabric/elasticell/pkg/storage"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var (
	storeBasePort    = 50000
	storeAddrPattern = "127.0.0.1:%d"
)

type baseSuite struct {
	client  *pd.Client
	servers []*pdserver.Server
}

func (s *baseSuite) stopMultiPDServers(c *C) {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}

	if s.servers != nil {
		for _, s := range s.servers {
			s.Stop()
			os.RemoveAll(s.GetCfg().DataDir)
		}

		s.servers = nil
	}
}

func (s *baseSuite) restartMultiPDServer(c *C, count int) uint64 {
	s.stopMultiPDServers(c)

	file, _ := ioutil.TempDir("", "ectd-log")
	f, err := os.Open(file)
	c.Assert(err, IsNil)

	log.InitLog()
	log.SetLevelByString("info")
	pdserver.RedirectEmbedEctdLog(f)

	s.servers = pdserver.NewTestMultiServers(count)
	var addrs []string
	var wg sync.WaitGroup
	for index := 0; index < count; index++ {
		addrs = append(addrs, s.servers[index].GetCfg().RPCAddr)
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			s.servers[index].Start()
		}(index)
	}

	wg.Wait()

	s.client, err = pd.NewClient("test-pd-cli", addrs...)
	c.Assert(err, IsNil)

	rsp, err := s.client.GetClusterID(context.TODO(), new(pdpb.GetClusterIDReq))
	c.Assert(err, IsNil)
	return rsp.ID
}

func (s *baseSuite) bootstrap(c *C, clusterID, storeID uint64) *Store {
	driver := s.newStoreDriver()
	cfg, storeMeta := s.newStoreMeta(storeID)

	peer := metapb.Peer{
		ID:      s.AllocID(c),
		StoreID: storeID,
	}

	cell := metapb.Cell{
		ID:    s.AllocID(c),
		Peers: []*metapb.Peer{&peer},
	}

	rsp, err := s.client.BootstrapCluster(context.TODO(), &pdpb.BootstrapClusterReq{
		Store: storeMeta,
		Cell:  cell,
	})
	c.Assert(err, IsNil)
	c.Assert(rsp.AlreadyBootstrapped, IsFalse)

	err = SaveFirstCell(driver, cell)
	c.Assert(err, IsNil)

	store := NewStore(clusterID, s.client, storeMeta, driver, cfg)
	store.Start()

	req := new(pdpb.PutStoreReq)
	req.Header.ClusterID = clusterID
	req.Store = storeMeta
	_, err = s.client.PutStore(context.TODO(), req)
	c.Assert(err, IsNil)

	return store
}

func (s *baseSuite) startFirstRaftGroup(c *C, num int) []*Store {
	clusterID := s.restartMultiPDServer(c, num)

	var stores []*Store
	stores = append(stores, s.bootstrap(c, clusterID, s.AllocID(c)))

	for index := 0; index < num-1; index++ {
		stores = append(stores, s.startNewStore(c, clusterID, s.newStoreDriver()))
	}

	return stores
}

func (s *baseSuite) checkPeers(c *C, num int, stores []*Store) *Store {
	time.Sleep(stores[0].cfg.getCellHeartbeatDuration() * 30)
	var leader *Store
	leaderCnt := 0

	for _, store := range stores {
		c.Assert(store.replicatesMap.size(), Equals, uint32(1))
		store.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
			c.Assert(len(pr.getCell().Peers), Equals, num)
			if pr.isLeader() {
				leaderCnt++
				leader = pr.store
			}
			return false, nil
		})
		c.Assert(len(store.peerCache.m), Equals, num)
	}

	c.Assert(leader, NotNil)
	c.Assert(leaderCnt, Equals, 1)
	return leader
}

func (s *baseSuite) newStoreDriver() storage.Driver {
	return storage.NewMemoryDriver()
}

func (s *baseSuite) AllocID(c *C) uint64 {
	rsp, err := s.client.AllocID(context.TODO(), &pdpb.AllocIDReq{})
	c.Assert(err, IsNil)

	if err != nil {
		c.Fatal("alloc id from pd servers error", err)
	}

	return rsp.ID
}

func (s *baseSuite) newStoreMeta(storeID uint64) (*Cfg, metapb.Store) {
	cfg := newTestStoreCfg(storeID)

	return cfg, metapb.Store{
		ID:      storeID,
		Address: cfg.StoreAddr,
		Lables: []metapb.Label{
			newLable("rack", fmt.Sprintf("%d", storeID)),
			newLable("zone", "test"),
		},
		State: metapb.UP,
	}
}

func (s *baseSuite) startNewStore(c *C, clusterID uint64, driver storage.Driver) *Store {
	storeID := s.AllocID(c)
	cfg, storeMeta := s.newStoreMeta(storeID)
	store := NewStore(clusterID, s.client, storeMeta, driver, cfg)
	store.Start()

	req := new(pdpb.PutStoreReq)
	req.Header.ClusterID = clusterID
	req.Store = storeMeta
	_, err := s.client.PutStore(context.TODO(), req)
	c.Assert(err, IsNil)
	return store
}

var lock sync.Mutex

func newTestStoreCfg(id uint64) *Cfg {
	lock.Lock()
	defer lock.Unlock()

	c := new(Cfg)
	c.StoreAddr = fmt.Sprintf(storeAddrPattern, storeBasePort)
	c.StoreAdvertiseAddr = c.StoreAddr
	c.StoreDataPath, _ = ioutil.TempDir("", fmt.Sprintf("%d", id))
	c.StoreHeartbeatIntervalMs = 10000
	c.CellHeartbeatIntervalMs = 60000
	c.MaxPeerDownSec = 300
	c.SplitCellCheckIntervalMs = 5
	c.RaftGCLogIntervalMs = 10000

	c.CellSplitSize = 96 * 1024 * 1024
	c.RaftLogGCCountLimit = c.CellSplitSize * 3 / 4 / 1024
	c.RaftLogGCSizeLimit = int64(c.CellSplitSize) * 3 / 4
	c.RaftLogGCThreshold = 50
	c.CellCheckSizeDiff = int64(c.CellSplitSize) / 8
	c.CellMaxSize = c.CellSplitSize / 2 * 3

	c.Raft = new(RaftCfg)
	c.Raft.ElectionTick = 2
	c.Raft.BaseTick = 1000
	c.Raft.HeartbeatTick = 1
	c.Raft.MaxSizePerMsg = 1024 * 1024
	c.Raft.MaxSizePerEntry = 8 * 1024 * 1024
	c.Raft.MaxInflightMsgs = 256

	storeBasePort++

	c.MaxPeerDownSec = 1
	c.StoreHeartbeatIntervalMs = 50
	c.CellHeartbeatIntervalMs = 50
	c.Raft.BaseTick = 20
	c.CellSplitSize = 1024
	c.CellMaxSize = 1024
	c.CellCheckSizeDiff = 1024

	return c
}

func newLable(key, value string) metapb.Label {
	return metapb.Label{
		Key:   key,
		Value: value,
	}
}
