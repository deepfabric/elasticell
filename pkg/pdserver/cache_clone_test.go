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
	s := newStoreInfo(t.newTestStore(1, ":10000"))
	s2 := s.clone()

	s.Meta.Address = ":10001"
	c.Assert(s2.Meta.Address == s.Meta.Address, IsFalse)

	s.Meta.Lables = []metapb.Label{
		metapb.Label{
			Key:   "1",
			Value: "2",
		},
	}
	c.Assert(len(s2.Meta.Lables) == len(s.Meta.Lables), IsFalse)

	s.Status.LeaderCount = 100
	c.Assert(s2.Status.LeaderCount == s.Status.LeaderCount, IsFalse)

	s.Status.Stats.Capacity = 100
	c.Assert(s2.Status.Stats.Capacity == s.Status.Stats.Capacity, IsFalse)
}
