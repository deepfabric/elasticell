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

type testLeaderStoreSuite struct {
	store LeaderStore
}

func (s *testLeaderStoreSuite) SetUpSuite(c *C) {
	s.store = _testSingleSvr.store
}

func (s *testLeaderStoreSuite) TearDownSuite(c *C) {}

func (s *testLeaderStoreSuite) TestGetCurrentLeader(c *C) {
	leader, err := s.store.GetCurrentLeader()
	c.Assert(err, IsNil)
	c.Assert(leader.ID, Equals, _testSingleSvr.id)
}
