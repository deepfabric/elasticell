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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	. "github.com/pingcap/check"
)

func (t *testCacheSuite) TestClone(c *C) {
	s := newStoreRuntime(t.newTestStore(1, ":10000"))
	s2 := s.clone()

	s.store.Address = ":10001"
	c.Assert(s2.store.Address == s.store.Address, IsFalse)

	s.store.Lables = []metapb.Label{
		metapb.Label{
			Key:   "1",
			Value: "2",
		},
	}
	c.Assert(len(s2.store.Lables) == len(s.store.Lables), IsFalse)

	s.status.LeaderCount = 100
	c.Assert(s2.status.LeaderCount == s.status.LeaderCount, IsFalse)

	s.status.stats.Capacity = 100
	c.Assert(s2.status.stats.Capacity == s.status.stats.Capacity, IsFalse)
}
