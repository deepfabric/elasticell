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

	"github.com/deepfabric/elasticell/pkg/util"
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
	key := []byte("TestZAdd")
	score := 10.0
	m1 := []byte("m1")

	n, err := s.driver.GetZSetEngine().ZAdd(key, score, m1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoZSetSuite) TestZCard(c *C) {
	key := []byte("TestZCard")
	score := 10.0
	m1 := []byte("m1")

	n, err := s.driver.GetZSetEngine().ZCard(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	n, err = s.driver.GetZSetEngine().ZAdd(key, score, m1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = s.driver.GetZSetEngine().ZCard(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoZSetSuite) TestZCount(c *C) {
	key := []byte("TestZCount")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	n, err := s.driver.GetZSetEngine().ZCount(key, score1, score3)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score2, m3)

	n, err = s.driver.GetZSetEngine().ZCount(key, score1, score3)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))
}

func (s *testNemoZSetSuite) TestZIncrBy(c *C) {
	key := []byte("TestZIncrBy")
	score := 1.0
	m1 := []byte("m1")

	v, err := s.driver.GetZSetEngine().ZIncrBy(key, m1, 1.0)
	c.Assert(err, IsNil)
	vf, _ := util.StrFloat64(v)
	c.Assert(int(vf), Equals, 1)

	s.driver.GetZSetEngine().ZAdd(key, score, m1)
	v, err = s.driver.GetZSetEngine().ZIncrBy(key, m1, 1.0)
	c.Assert(err, IsNil)
	vf, _ = util.StrFloat64(v)
	c.Assert(int(vf), Equals, 2)
}

func (s *testNemoZSetSuite) TestZLexCount(c *C) {

}

func (s *testNemoZSetSuite) TestZRange(c *C) {
	key := []byte("TestZRange")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	values, err := s.driver.GetZSetEngine().ZRange(key, 0, -1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	values, err = s.driver.GetZSetEngine().ZRange(key, 0, -1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)

	values, err = s.driver.GetZSetEngine().ZRange(key, 1, -1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)

	values, err = s.driver.GetZSetEngine().ZRange(key, 2, -1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 1)

	values, err = s.driver.GetZSetEngine().ZRange(key, 3, -1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)
}

func (s *testNemoZSetSuite) TestZRangeByLex(c *C) {

}

func (s *testNemoZSetSuite) TestZRangeByScore(c *C) {
	// key := []byte("TestZRangeByScore")
	// score1 := 1.0
	// m1 := []byte("m1")

	// score2 := 2.0
	// m2 := []byte("m2")

	// score3 := 3.0
	// m3 := []byte("m3")
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
