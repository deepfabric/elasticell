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

// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	"os"

	. "github.com/pingcap/check"
)

type testNemoZSetSuite struct {
	driver Driver
}

func (s *testNemoZSetSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver("/tmp/nemo-zset")
	c.Assert(err, IsNil)
}

func (s *testNemoZSetSuite) TearDownSuite(c *C) {
	err := os.RemoveAll("/tmp/nemo-zset")
	c.Assert(err, IsNil)
}

func (s *testNemoZSetSuite) TestZAdd(c *C) {

}

func (s *testNemoZSetSuite) TestZCard(c *C) {

}

func (s *testNemoZSetSuite) TestZCount(c *C) {

}

func (s *testNemoZSetSuite) TestZIncrBy(c *C) {

}

func (s *testNemoZSetSuite) TestZLexCount(c *C) {

}

func (s *testNemoZSetSuite) TestZRange(c *C) {

}

func (s *testNemoZSetSuite) TestZRangeByLex(c *C) {

}

func (s *testNemoZSetSuite) TestZRangeByScore(c *C) {

}

func (s *testNemoZSetSuite) TestZRank(c *C) {

}

func (s *testNemoZSetSuite) TestZRem(c *C) {

}

func (s *testNemoZSetSuite) TestZRemRangeByLex(c *C) {

}

func (s *testNemoZSetSuite) TestZRemRangeByRank(c *C) {

}

func (s *testNemoZSetSuite) TestZRemRangeByScore(c *C) {

}

func (s *testNemoZSetSuite) TestZScore(c *C) {

}
