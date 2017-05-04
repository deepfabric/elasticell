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
	"testing"

	"time"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) { TestingT(t) }

type testAllocSuite struct{}

var _ = Suite(&testAllocSuite{})

func (s *testAllocSuite) TestAlloc(c *C) {
	svr := NewTestSingleServer()
	svr.Start()
	defer svr.Stop()

	time.Sleep(time.Second * 1)

	alloc := svr.idAlloc

	id, err := alloc.newID()
	c.Assert(err, IsNil)
	c.Assert(id, Equals, uint64(1))
}
