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
	"sync"

	. "github.com/pingcap/check"
)

type testClusterStoreSuite struct {
	store ClusterStore
}

func (s *testClusterStoreSuite) SetUpSuite(c *C) {
	s.store = _testSingleSvr.store
}

func (s *testClusterStoreSuite) TearDownSuite(c *C) {

}

func (s *testClusterStoreSuite) TestCreateFirstClusterID(c *C) {
	var wg sync.WaitGroup

	for index := 0; index < 5; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := s.store.CreateFirstClusterID()
			c.Assert(err, IsNil)
			c.Assert(id, Equals, _testSingleSvr.GetClusterID())
		}()
	}

	wg.Wait()
}

func (s *testClusterStoreSuite) TestGetClusterID(c *C) {
	id, err := s.store.GetClusterID()
	c.Assert(err, IsNil)
	c.Assert(id, Equals, _testSingleSvr.GetClusterID())
}

func (s *testClusterStoreSuite) TestGetCurrentClusterMembers(c *C) {
	members, err := s.store.GetCurrentClusterMembers()
	c.Assert(err, IsNil)
	c.Assert(len(members.Members), Equals, 1)
}
