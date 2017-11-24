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

type testNemoListSuite struct {
	driver Driver
}

func (s *testNemoListSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver(&NemoCfg{
		DataPath: "/tmp/nemo-list",
	})
	c.Assert(err, IsNil)
}

func (s *testNemoListSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-list")
}

func (s *testNemoListSuite) TestLIndex(c *C) {
	key := []byte("TestLIndex")
	member := []byte("member1")

	v, err := s.driver.GetListEngine().LIndex(key, 0)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	s.driver.GetListEngine().LPush(key, member)

	v, err = s.driver.GetListEngine().LIndex(key, 0)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member))

	v, err = s.driver.GetListEngine().LIndex(key, 1)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoListSuite) TestLInsert(c *C) {
	before := 0
	after := 1

	key := []byte("TestLInsert")
	member := []byte("member1")
	pivot := []byte("member3")
	insert := []byte("member2")
	insert2 := []byte("member4")

	n, err := s.driver.GetListEngine().LInsert(key, before, pivot, insert)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetListEngine().LInsert(key, after, pivot, insert)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetListEngine().LPush(key, pivot)
	s.driver.GetListEngine().LPush(key, member)

	n, err = s.driver.GetListEngine().LInsert(key, before, pivot, insert)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))

	n, err = s.driver.GetListEngine().LInsert(key, after, pivot, insert2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(4))

	v, _ := s.driver.GetListEngine().LIndex(key, 1)
	c.Assert(string(v), Equals, string(insert))

	v, _ = s.driver.GetListEngine().LIndex(key, -1)
	c.Assert(string(v), Equals, string(insert2))
}

func (s *testNemoListSuite) TestLLen(c *C) {
	key := []byte("TestLLen")
	member1 := []byte("member1")
	member2 := []byte("member2")

	n, err := s.driver.GetListEngine().LLen(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetListEngine().LPush(key, member2)
	s.driver.GetListEngine().LPush(key, member1)

	n, err = s.driver.GetListEngine().LLen(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoListSuite) TestLPop(c *C) {
	key := []byte("TestLPop")
	member1 := []byte("member1")
	member2 := []byte("member2")

	v, err := s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	s.driver.GetListEngine().LPush(key, member2)
	s.driver.GetListEngine().LPush(key, member1)

	v, err = s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member1))

	v, err = s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member2))

	v, err = s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoListSuite) TestLPush(c *C) {
	key := []byte("TestLPush")
	member1 := []byte("member1")
	member2 := []byte("member2")

	n, err := s.driver.GetListEngine().LPush(key, member2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = s.driver.GetListEngine().LPush(key, member1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	v, err := s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member1))

	v, err = s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member2))

	v, err = s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoListSuite) TestLPushX(c *C) {
	key := []byte("TestLPushX")
	member1 := []byte("member1")
	member2 := []byte("member2")
	member3 := []byte("member3")

	n, err := s.driver.GetListEngine().LPushX(key, member2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetListEngine().LPush(key, member3)

	n, err = s.driver.GetListEngine().LPushX(key, member2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	n, err = s.driver.GetListEngine().LPushX(key, member1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(3))

	v, err := s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member1))

	v, err = s.driver.GetListEngine().LPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member2))
}

func (s *testNemoListSuite) TestLRange(c *C) {
	key := []byte("TestLRange")
	member1 := []byte("member1")
	member2 := []byte("member2")

	values, err := s.driver.GetListEngine().LRange(key, 0, 1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetListEngine().LPush(key, member2)
	s.driver.GetListEngine().LPush(key, member1)

	values, err = s.driver.GetListEngine().LRange(key, 0, 0)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 1)

	values, err = s.driver.GetListEngine().LRange(key, 0, 1)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)
}

func (s *testNemoListSuite) TestLRem(c *C) {
	key := []byte("TestLRem")
	member1 := []byte("member1")
	member2 := []byte("member2")
	member3 := []byte("member1")

	n, err := s.driver.GetListEngine().LRem(key, 1, member2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetListEngine().LPush(key, member3)
	s.driver.GetListEngine().LPush(key, member2)
	s.driver.GetListEngine().LPush(key, member1)

	n, err = s.driver.GetListEngine().LRem(key, 1, member2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = s.driver.GetListEngine().LRem(key, 2, member1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoListSuite) TestLSet(c *C) {
	key := []byte("TestLSet")
	member1 := []byte("member1")
	member2 := []byte("member2")

	err := s.driver.GetListEngine().LSet(key, 0, member2)
	c.Assert(err, NotNil)

	s.driver.GetListEngine().LPush(key, member2)

	err = s.driver.GetListEngine().LSet(key, 0, member1)
	c.Assert(err, IsNil)
}

func (s *testNemoListSuite) TestLTrim(c *C) {
	key := []byte("TestLTrim")
	member1 := []byte("member1")
	member2 := []byte("member2")

	err := s.driver.GetListEngine().LTrim(key, 1, -1)
	c.Assert(err, IsNil)

	s.driver.GetListEngine().LPush(key, member2)
	s.driver.GetListEngine().LPush(key, member1)

	err = s.driver.GetListEngine().LTrim(key, 1, -1)
	c.Assert(err, IsNil)

	n, _ := s.driver.GetListEngine().LLen(key)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoListSuite) TestRPop(c *C) {
	key := []byte("TestRPop")
	member1 := []byte("member1")
	member2 := []byte("member2")

	v, err := s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	s.driver.GetListEngine().RPush(key, member2)
	s.driver.GetListEngine().RPush(key, member1)

	v, err = s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member1))

	v, err = s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member2))

	v, err = s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoListSuite) TestRPush(c *C) {
	key := []byte("TestRPush")
	member1 := []byte("member1")
	member2 := []byte("member2")

	s.driver.GetListEngine().RPush(key, member2)
	s.driver.GetListEngine().RPush(key, member1)

	v, err := s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member1))

	v, err = s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(member2))

	v, err = s.driver.GetListEngine().RPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoListSuite) TestRPushX(c *C) {
	key := []byte("TestRPush")
	member1 := []byte("member1")
	member2 := []byte("member2")

	n, err := s.driver.GetListEngine().RPushX(key, member1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetListEngine().RPush(key, member2)

	n, err = s.driver.GetListEngine().RPushX(key, member1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}
