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

type testRemoveSuite struct {
	baseSuite
}

func (s *testRemoveSuite) TestRemove(c *C) {
	stores := s.startFirstRaftGroup(c, 3)

	defer func() {
		for _, store := range stores {
			store.Stop()
		}
		s.stopMultiPDServers(c)
	}()

	s.checkPeers(c, 3, stores)
	stores[0].Stop()

	time.Sleep(stores[0].cfg.getMaxPeerDownSecDuration() * 10)
	c.Assert(len(stores[1].peerCache.m), Equals, 2)
	c.Assert(len(stores[2].peerCache.m), Equals, 2)
}
