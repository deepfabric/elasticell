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

var _ = Suite(&testIDStoreSuite{})

type testIDStoreSuite struct {
	store IDStore
}

func (s *testIDStoreSuite) SetUpSuite(c *C) {
	s.store = _testSingleSvr.store
}

func (s *testIDStoreSuite) TearDownSuite(c *C) {

}

func (s *testIDStoreSuite) TestGet(c *C) {
	id, err := s.store.GetID()
	c.Assert(err, IsNil)
	c.Assert(id, Greater, uint64(0))
}
