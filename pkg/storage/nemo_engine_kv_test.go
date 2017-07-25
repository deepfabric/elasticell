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

type testNemoKVSuite struct {
	driver Driver
}

func (s *testNemoKVSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver("/tmp/nemo-kv")
	c.Assert(err, IsNil)
}

func (s *testNemoKVSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-kv")
}

func (s *testNemoKVSuite) TestSet(c *C) {
	key := []byte("key1")
	value := []byte("value1")

	err := s.driver.GetKVEngine().Set(key, value)
	c.Assert(err, IsNil)
}

func (s *testNemoKVSuite) TestGet(c *C) {
	key := []byte("TestGet")
	value := []byte("value2")

	v, err := s.driver.GetKVEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	err = s.driver.GetKVEngine().Set(key, value)
	c.Assert(err, IsNil)

	v, err = s.driver.GetKVEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(value))
}

func (s *testNemoKVSuite) TestIncrBy(c *C) {
	key := []byte("TestIncrBy")

	err := s.driver.GetKVEngine().Set(key, []byte("1"))
	c.Assert(err, IsNil)

	n, err := s.driver.GetKVEngine().IncrBy(key, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoKVSuite) TestDecrBy(c *C) {
	key := []byte("TestDecrBy")

	err := s.driver.GetKVEngine().Set(key, []byte("2"))
	c.Assert(err, IsNil)

	n, err := s.driver.GetKVEngine().DecrBy(key, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoKVSuite) TestGetSet(c *C) {
	key := []byte("TestGetSet1")
	value := []byte("old-value")
	newValue := []byte("new-value")

	old, err := s.driver.GetKVEngine().GetSet(key, value)
	c.Assert(err, IsNil)
	c.Assert(len(old), Equals, 0)

	v, err := s.driver.GetKVEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(value))

	old, err = s.driver.GetKVEngine().GetSet(key, newValue)
	c.Assert(err, IsNil)
	c.Assert(string(old), Equals, string(value))

	v, err = s.driver.GetKVEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(newValue))
}

func (s *testNemoKVSuite) TestAppend(c *C) {
	key := []byte("TestAppend")
	value := []byte("value")

	n, err := s.driver.GetKVEngine().Append(key, value)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(len(value)))

	n, err = s.driver.GetKVEngine().Append(key, value)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2*len(value)))
}

func (s *testNemoKVSuite) TestSetNX(c *C) {
	key := []byte("TestSetNX")
	value := []byte("value")

	n, err := s.driver.GetKVEngine().SetNX(key, value)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	n, err = s.driver.GetKVEngine().SetNX(key, value)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))
}

func (s *testNemoKVSuite) TestStrLen(c *C) {
	key := []byte("TestStrLen")
	value := []byte("value")

	n, err := s.driver.GetKVEngine().StrLen(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	err = s.driver.GetKVEngine().Set(key, value)
	c.Assert(err, IsNil)

	n, err = s.driver.GetKVEngine().StrLen(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(len(value)))
}
