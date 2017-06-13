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

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/redis"
	. "github.com/pingcap/check"
)

func (s *testStoreSuite) TestKVSet(c *C) {
	stores := s.startFirstRaftGroup(c, 3)

	defer func() {
		for _, store := range stores {
			store.Stop()
		}
		s.stopMultiPDServers(c)
	}()

	leader := s.checkPeers(c, 3, stores)

	complete := make(chan struct{}, 1)
	cmd := [][]byte{[]byte("set"), []byte("key1"), []byte("value1")}
	leader.OnRedisCommand(raftcmdpb.Set, redis.Command(cmd), func(resp *raftcmdpb.RaftCMDResponse) {
		c.Assert(len(resp.Responses), Equals, 1)
		c.Assert(resp.Responses[0].StatusResult, NotNil)
		complete <- struct{}{}
	})

	select {
	case <-complete:
	case <-time.After(time.Second):
		c.Fatal("set command timeout")
	}
}
