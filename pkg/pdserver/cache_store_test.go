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
	"errors"

	. "github.com/pingcap/check"
)

func (t *testCacheSuite) TestAddStore(c *C) {
	cache := t.newTestCache()
	s := t.newTestStore(1, ":12345")
	cache.getStoreCache().createStoreInfo(s)

	info := cache.getStoreCache().getStore(1)
	c.Assert(info, NotNil)
}

func (t *testCacheSuite) TestForeachStore(c *C) {
	var storeID uint64
	storeID = 100

	cache := t.newTestCache()
	s := t.newTestStore(storeID, ":11111")
	cache.getStoreCache().createStoreInfo(s)

	storeID++
	s = t.newTestStore(storeID, ":11111")
	cache.getStoreCache().createStoreInfo(s)

	cnt := 0
	err := cache.getStoreCache().foreach(func(s *StoreInfo) (bool, error) {
		cnt++
		return true, nil
	})
	c.Assert(cnt, Equals, 2)
	c.Assert(err, IsNil)

	cnt = 0
	err = cache.getStoreCache().foreach(func(s *StoreInfo) (bool, error) {
		cnt++
		if cnt == 1 {
			return false, nil
		}

		return true, nil
	})
	c.Assert(cnt, Equals, 1)
	c.Assert(err, IsNil)

	cnt = 0
	err = cache.getStoreCache().foreach(func(s *StoreInfo) (bool, error) {
		cnt++
		return true, errors.New("")
	})
	c.Assert(cnt, Equals, 1)
	c.Assert(err, NotNil)
}

func (t *testCacheSuite) TestGetStores(c *C) {
	cache := t.newTestCache()

	var storeID uint64 = 100

	s := t.newTestStore(storeID, ":11111")
	cache.getStoreCache().createStoreInfo(s)

	storeID++
	s = t.newTestStore(storeID, ":11111")
	cache.getStoreCache().createStoreInfo(s)
	cache.getStoreCache().createStoreInfo(s)
	cache.getStoreCache().createStoreInfo(s)

	stores := cache.getStoreCache().getStores()
	c.Assert(stores, NotNil)
	c.Assert(len(stores), Equals, 2)
}
