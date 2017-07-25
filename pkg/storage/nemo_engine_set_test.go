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

type testNemoSetSuite struct {
	driver Driver
}

func (s *testNemoSetSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver("/tmp/nemo-set")
	c.Assert(err, IsNil)
}

func (s *testNemoSetSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-set")
}

func (s *testNemoSetSuite) TestSAdd(c *C) {
	key := []byte("TestSAdd")
	m1 := []byte("m1")
	m2 := []byte("m2")

	n, err := s.driver.GetSetEngine().SAdd(key, m1, m2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoSetSuite) TestSRem(c *C) {
	key := []byte("TestSRem")
	m1 := []byte("m1")
	m2 := []byte("m2")

	s.driver.GetSetEngine().SAdd(key, m1, m2)

	n, err := s.driver.GetSetEngine().SRem(key, m1, m2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoSetSuite) TestSCard(c *C) {
	key := []byte("TestSCard")
	m1 := []byte("m1")
	m2 := []byte("m2")

	n, err := s.driver.GetSetEngine().SCard(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetSetEngine().SAdd(key, m1, m2)

	n, err = s.driver.GetSetEngine().SCard(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoSetSuite) TestSMembers(c *C) {
	key := []byte("TestSMembers")
	m1 := []byte("m1")
	m2 := []byte("m2")

	values, err := s.driver.GetSetEngine().SMembers(key)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetSetEngine().SAdd(key, m1, m2)

	values, err = s.driver.GetSetEngine().SMembers(key)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)
}

func (s *testNemoSetSuite) TestSIsMember(c *C) {
	key := []byte("TestSIsMember")
	m1 := []byte("m1")
	m2 := []byte("m2")

	n, err := s.driver.GetSetEngine().SIsMember(key, m1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetSetEngine().SAdd(key, m1, m2)

	n, err = s.driver.GetSetEngine().SIsMember(key, m1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = s.driver.GetSetEngine().SIsMember(key, m2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoSetSuite) TestSPop(c *C) {
	key := []byte("TestSPop")
	m1 := []byte("m1")
	m2 := []byte("m2")

	v, err := s.driver.GetSetEngine().SPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	s.driver.GetSetEngine().SAdd(key, m1, m2)

	v, err = s.driver.GetSetEngine().SPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v) > 0, IsTrue)

	v, err = s.driver.GetSetEngine().SPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v) > 0, IsTrue)

	v, err = s.driver.GetSetEngine().SPop(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}
