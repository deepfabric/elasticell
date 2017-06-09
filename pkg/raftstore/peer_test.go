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

func (s *testStoreSuite) TestPeer(c *C) {
	clusterID := s.restartMultiPDServer(c, 3)

	store := s.bootstrap(c, clusterID, s.AllocID(c))
	store2 := s.startNewStore(c, clusterID, s.newStoreDriver())
	store3 := s.startNewStore(c, clusterID, s.newStoreDriver())

	defer func() {
		store.Stop()
		store2.Stop()
		store3.Stop()
		s.stopMultiPDServers(c)
	}()

	time.Sleep(store.cfg.getCellHeartbeatDuration() * 5)
	c.Assert(store.replicatesMap.size(), Equals, uint32(1))
	c.Assert(store2.replicatesMap.size(), Equals, uint32(1))
	c.Assert(store3.replicatesMap.size(), Equals, uint32(1))

	store.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		c.Assert(len(pr.getCell().Peers), Equals, 3)
		return false, nil
	})

	store2.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		c.Assert(len(pr.getCell().Peers), Equals, 3)
		return false, nil
	})

	store3.replicatesMap.foreach(func(pr *PeerReplicate) (bool, error) {
		c.Assert(len(pr.getCell().Peers), Equals, 3)
		return false, nil
	})
}
