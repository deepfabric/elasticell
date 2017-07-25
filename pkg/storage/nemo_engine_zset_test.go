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
	os.RemoveAll("/tmp/nemo-zset")
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

	min := []byte("1")
	max := []byte("3")
	n, err := s.driver.GetZSetEngine().ZCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	min = []byte("1")
	max = []byte("3")
	n, err = s.driver.GetZSetEngine().ZCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))

	min = []byte("(1")
	max = []byte("3")
	n, err = s.driver.GetZSetEngine().ZCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	min = []byte("(1")
	max = []byte("(3")
	n, err = s.driver.GetZSetEngine().ZCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
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
	key := []byte("TestZLexCount")
	score := 1.0
	m1 := []byte("a")
	m2 := []byte("b")
	m3 := []byte("c")

	min := []byte("[a")
	max := []byte("[c")
	n, err := s.driver.GetZSetEngine().ZLexCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score, m1)
	s.driver.GetZSetEngine().ZAdd(key, score, m2)
	s.driver.GetZSetEngine().ZAdd(key, score, m3)

	min = []byte("[a")
	max = []byte("[c")
	n, err = s.driver.GetZSetEngine().ZLexCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))

	min = []byte("(a")
	max = []byte("(c")
	n, err = s.driver.GetZSetEngine().ZLexCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	min = []byte("(a")
	max = []byte("[c")
	n, err = s.driver.GetZSetEngine().ZLexCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	min = []byte("[a")
	max = []byte("(c")
	n, err = s.driver.GetZSetEngine().ZLexCount(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
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
	key := []byte("TestZRangeByLex")
	score := 0.0

	m1 := []byte("a")
	m2 := []byte("b")
	m3 := []byte("c")

	min := []byte("[a")
	max := []byte("[c")
	values, err := s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetZSetEngine().ZAdd(key, score, m1)
	s.driver.GetZSetEngine().ZAdd(key, score, m2)
	s.driver.GetZSetEngine().ZAdd(key, score, m3)

	min = []byte("[a")
	max = []byte("[c")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)

	min = []byte("[a")
	max = []byte("(c")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)

	min = []byte("(a")
	max = []byte("(c")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 1)

	min = []byte("-")
	max = []byte("(c")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)

	min = []byte("-")
	max = []byte("[c")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)

	min = []byte("(a")
	max = []byte("+")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)

	min = []byte("[a")
	max = []byte("+")
	values, err = s.driver.GetZSetEngine().ZRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)
}

func (s *testNemoZSetSuite) TestZRangeByScore(c *C) {
	key := []byte("TestZRangeByScore")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("1")
	max := []byte("3")
	values, err := s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	min = []byte("1")
	max = []byte("3")
	values, err = s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)

	min = []byte("(1")
	max = []byte("3")
	values, err = s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)

	min = []byte("(1")
	max = []byte("(3")
	values, err = s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 1)

	min = []byte("-inf")
	max = []byte("+inf")
	values, err = s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)

	min = []byte("-inf")
	max = []byte("3")
	values, err = s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)

	min = []byte("1")
	max = []byte("+inf")
	values, err = s.driver.GetZSetEngine().ZRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 3)
}

func (s *testNemoZSetSuite) TestZRank(c *C) {
	key := []byte("TestZRank")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	m4 := []byte("m4")

	index, err := s.driver.GetZSetEngine().ZRank(key, m1)
	c.Assert(err, IsNil)
	c.Assert(index < 0, IsTrue)

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	index, err = s.driver.GetZSetEngine().ZRank(key, m1)
	c.Assert(err, IsNil)
	c.Assert(index, Equals, int64(0))

	index, err = s.driver.GetZSetEngine().ZRank(key, m2)
	c.Assert(err, IsNil)
	c.Assert(index, Equals, int64(1))

	index, err = s.driver.GetZSetEngine().ZRank(key, m3)
	c.Assert(err, IsNil)
	c.Assert(index, Equals, int64(2))

	index, err = s.driver.GetZSetEngine().ZRank(key, m4)
	c.Assert(err, IsNil)
	c.Assert(index < 0, IsTrue)
}

func (s *testNemoZSetSuite) TestZRem(c *C) {
	key := []byte("TestZRem")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	count, err := s.driver.GetZSetEngine().ZRem(key, m1)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	count, err = s.driver.GetZSetEngine().ZRem(key, m1)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))

	count, err = s.driver.GetZSetEngine().ZRem(key, m2, m3)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	count, err = s.driver.GetZSetEngine().ZRem(key, m1, m2, m3)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))
}

func (s *testNemoZSetSuite) TestZRemRangeByLex(c *C) {
	key := []byte("TestZRemRangeByLex")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("m1")
	max := []byte("m3")
	count, err := s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("[m1")
	max = []byte("[m3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("(m1")
	max = []byte("[m3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	min = []byte("[m2")
	max = []byte("(m3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	min = []byte("[m1")
	max = []byte("(m3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))

	min = []byte("(m3")
	max = []byte("+")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("-")
	max = []byte("(m1")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	min = []byte("-")
	max = []byte("[m3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByLex(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))
}

func (s *testNemoZSetSuite) TestZRemRangeByRank(c *C) {
	key := []byte("TestZRemRangeByRank")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	count, err := s.driver.GetZSetEngine().ZRemRangeByRank(key, 0, -1)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	count, err = s.driver.GetZSetEngine().ZRemRangeByRank(key, 0, -1)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))

	count, err = s.driver.GetZSetEngine().ZRemRangeByRank(key, 0, -1)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	count, err = s.driver.GetZSetEngine().ZRemRangeByRank(key, 0, 1)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	count, err = s.driver.GetZSetEngine().ZRemRangeByRank(key, 0, 0)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))
}

func (s *testNemoZSetSuite) TestZRemRangeByScore(c *C) {
	key := []byte("TestZRemRangeByScore")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	min := []byte("[1")
	max := []byte("[3")
	count, err := s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	min = []byte("[1")
	max = []byte("[3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))

	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(0))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("(1")
	max = []byte("[3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("(1")
	max = []byte("(3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("[1")
	max = []byte("(3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("-inf")
	max = []byte("[3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("-inf")
	max = []byte("(3")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("[1")
	max = []byte("+inf")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("(1")
	max = []byte("+inf")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(2))

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)
	min = []byte("-inf")
	max = []byte("+inf")
	count, err = s.driver.GetZSetEngine().ZRemRangeByScore(key, min, max)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(3))
}

func (s *testNemoZSetSuite) TestZScore(c *C) {
	key := []byte("TestZScore")
	score1 := 1.0
	m1 := []byte("m1")

	score2 := 2.0
	m2 := []byte("m2")

	score3 := 3.0
	m3 := []byte("m3")

	tmp, err := s.driver.GetZSetEngine().ZScore(key, m1)
	c.Assert(err, IsNil)
	c.Assert(tmp, IsNil)

	s.driver.GetZSetEngine().ZAdd(key, score1, m1)
	s.driver.GetZSetEngine().ZAdd(key, score2, m2)
	s.driver.GetZSetEngine().ZAdd(key, score3, m3)

	tmp, err = s.driver.GetZSetEngine().ZScore(key, m1)
	score, _ := util.StrFloat64(tmp)
	c.Assert(err, IsNil)
	c.Assert(int(score), Equals, 1)

	tmp, err = s.driver.GetZSetEngine().ZScore(key, m2)
	score, _ = util.StrFloat64(tmp)
	c.Assert(err, IsNil)
	c.Assert(int(score), Equals, 2)

	tmp, err = s.driver.GetZSetEngine().ZScore(key, m3)
	score, _ = util.StrFloat64(tmp)
	c.Assert(err, IsNil)
	c.Assert(int(score), Equals, 3)
}
