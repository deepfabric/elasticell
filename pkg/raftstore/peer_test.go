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

	. "github.com/pingcap/check"
)

func (s *testStoreSuite) startFirstRaftGroup(c *C, num int) []*Store {
	clusterID := s.restartMultiPDServer(c, num)

	var stores []*Store
	stores = append(stores, s.bootstrap(c, clusterID, s.AllocID(c)))

	for index := 0; index < num-1; index++ {
		stores = append(stores, s.startNewStore(c, clusterID, s.newStoreDriver()))
	}

	return stores
}

func (s *testStoreSuite) checkPeers(c *C, num int, stores []*Store) *Store {
	time.Sleep(stores[0].cfg.getCellHeartbeatDuration() * 10)
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
	}

	c.Assert(leader, NotNil)
	c.Assert(leaderCnt, Equals, 1)
	return leader
}

func (s *testStoreSuite) TestFirstGroup(c *C) {
	stores := s.startFirstRaftGroup(c, 3)

	defer func() {
		for _, store := range stores {
			store.Stop()
		}
		s.stopMultiPDServers(c)
	}()

	s.checkPeers(c, 3, stores)
}
