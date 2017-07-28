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
	. "github.com/pingcap/check"
)

func (t *testCacheSuite) TestAddCell(c *C) {
	cache := t.newTestCache()
	cache.getCellCache().createAndAdd(t.newTestCell(1, nil, nil, t.newTestPeer(2, 100)))
	info := cache.getCellCache().getCell(1)
	c.Assert(info, NotNil)
}

func (t *testCacheSuite) TestGetCellStores(c *C) {
	cache := t.newTestCache()

	cache.getStoreCache().createStoreInfo(t.newTestStore(1, ":11111"))
	cache.getStoreCache().createStoreInfo(t.newTestStore(2, ":11111"))
	cache.getStoreCache().createStoreInfo(t.newTestStore(4, ":11111"))

	cell := t.newTestCell(100, nil, nil, t.newTestPeer(99, 1), t.newTestPeer(99, 2), t.newTestPeer(99, 3))
	cache.getCellCache().createAndAdd(cell)

	cr := cache.getCellCache().getCell(100)
	stores := cache.getStoreCache().getCellStores(cr)
	c.Assert(stores, NotNil)
	c.Assert(len(stores), Equals, 2)

	cache.getStoreCache().createStoreInfo(t.newTestStore(3, ":11111"))
	stores = cache.getStoreCache().getCellStores(cr)
	c.Assert(stores, NotNil)
	c.Assert(len(stores), Equals, 3)
}

func (t *testCacheSuite) TestSearchCell(c *C) {
	cache := t.newTestCache()

	cache.getCellCache().createAndAdd(t.newTestCell(1, nil, []byte{10}, t.newTestPeer(100, 101)))
	cache.getCellCache().createAndAdd(t.newTestCell(2, []byte{10}, []byte{20}, t.newTestPeer(102, 103)))
	cache.getCellCache().createAndAdd(t.newTestCell(3, []byte{21}, []byte{30}, t.newTestPeer(104, 105)))
	cache.getCellCache().createAndAdd(t.newTestCell(4, []byte{30}, nil, t.newTestPeer(106, 107)))

	c.Assert(cache.getCellCache().searchCell([]byte{5}), NotNil)
	c.Assert(cache.getCellCache().searchCell([]byte{10}), NotNil)
	c.Assert(cache.getCellCache().searchCell([]byte{15}), NotNil)
	c.Assert(cache.getCellCache().searchCell([]byte{20}), IsNil)
	c.Assert(cache.getCellCache().searchCell([]byte{25}), NotNil)
	c.Assert(cache.getCellCache().searchCell([]byte{100}), NotNil)
}

func (t *testCacheSuite) TestGetFollowers(c *C) {
	cache := t.newTestCache()

	cache.getCellCache().createAndAdd(t.newTestCell(1, nil, nil,
		t.newTestPeer(100, 1000),
		t.newTestPeer(101, 1001),
		t.newTestPeer(102, 1002)))

	info := cache.getCellCache().getCell(1)
	c.Assert(info, NotNil)

	followers := info.getFollowers()
	c.Assert(followers, NotNil)
	c.Assert(len(followers), Equals, 3)

	cache.getCellCache().createAndAdd(t.newTestCell(2, nil, nil,
		t.newTestPeer(200, 2000),
		t.newTestPeer(201, 2001),
		t.newTestPeer(202, 2002)))
	info = cache.getCellCache().getCell(2)
	c.Assert(info, NotNil)
	info.LeaderPeer = t.newTestPeer(200, 2000)
	followers = info.getFollowers()
	c.Assert(followers, NotNil)
	c.Assert(len(followers), Equals, 2)

}
