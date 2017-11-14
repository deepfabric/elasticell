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
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	. "github.com/pingcap/check"
)

type testCacheSuite struct{}

var _ = Suite(&testCacheSuite{})

func (t *testCacheSuite) SetUpSuite(c *C) {
}

func (t *testCacheSuite) TearDownSuite(c *C) {
}

func (t *testCacheSuite) newTestCache() *cache {
	return newCache(100, _testSingleSvr.store, _testSingleSvr.idAlloc, newWatcherNotifier(time.Second))
}

func (t *testCacheSuite) newTestPeer(id, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		ID:      id,
		StoreID: storeID,
	}
}

func (t *testCacheSuite) newTestCell(id uint64, start, end []byte, peers ...*metapb.Peer) metapb.Cell {
	return metapb.Cell{
		ID:    id,
		Start: start,
		End:   end,
		Peers: peers,
	}
}

func (t *testCacheSuite) newTestStore(id uint64, addr string) metapb.Store {
	return metapb.Store{
		ID:      id,
		Address: addr,
		Lables:  []metapb.Label{},
		State:   metapb.UP,
	}
}

func (t *testCacheSuite) TestAllowPeer(c *C) {
	cache := t.newTestCache()
	_, err := cache.allocPeer(100, true)
	c.Assert(err, IsNil)
}
