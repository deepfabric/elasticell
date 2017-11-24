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

type testNemoHashSuite struct {
	driver Driver
}

func (s *testNemoHashSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver(&NemoCfg{
		DataPath: "/tmp/nemo-hash",
	})
	c.Assert(err, IsNil)
}

func (s *testNemoHashSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-hash")
}

func (s *testNemoHashSuite) TestHSet(c *C) {
	key := []byte("TestHSet")
	field := []byte("f1")
	value := []byte("v1")

	n, err := s.driver.GetHashEngine().HSet(key, field, value)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoHashSuite) TestHGet(c *C) {
	key := []byte("TestHGet")
	field := []byte("f1")
	value := []byte("v1")

	field2 := []byte("f2")

	_, err := s.driver.GetHashEngine().HSet(key, field, value)
	c.Assert(err, IsNil)

	v, err := s.driver.GetHashEngine().HGet(key, field)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(value))

	v, err = s.driver.GetHashEngine().HGet(key, field2)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoHashSuite) TestHDel(c *C) {
	key := []byte("TestHDel")
	field1 := []byte("f1")
	value1 := []byte("v1")

	field2 := []byte("f2")
	value2 := []byte("v2")

	n, err := s.driver.GetHashEngine().HDel(key, field1, field2)
	c.Assert(err, IsNil)

	s.driver.GetHashEngine().HSet(key, field1, value1)
	s.driver.GetHashEngine().HSet(key, field2, value2)

	n, err = s.driver.GetHashEngine().HDel(key, field1, field2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoHashSuite) TestHExists(c *C) {
	key := []byte("TestHExists")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")

	s.driver.GetHashEngine().HSet(key, field1, value1)

	yes, err := s.driver.GetHashEngine().HExists(key, field1)
	c.Assert(err, IsNil)
	c.Assert(yes, Equals, true)

	yes, err = s.driver.GetHashEngine().HExists(key, field2)
	c.Assert(err, IsNil)
	c.Assert(yes, Equals, false)
}

func (s *testNemoHashSuite) TestHKeys(c *C) {
	key := []byte("TestHKeys")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	keys, err := s.driver.GetHashEngine().HKeys(key)
	c.Assert(err, IsNil)
	c.Assert(len(keys), Equals, 0)

	s.driver.GetHashEngine().HSet(key, field1, value1)
	s.driver.GetHashEngine().HSet(key, field2, value2)

	keys, err = s.driver.GetHashEngine().HKeys(key)
	c.Assert(err, IsNil)
	c.Assert(len(keys), Equals, 2)
}

func (s *testNemoHashSuite) TestHVals(c *C) {
	key := []byte("TestHVals")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	values, err := s.driver.GetHashEngine().HVals(key)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetHashEngine().HSet(key, field1, value1)
	s.driver.GetHashEngine().HSet(key, field2, value2)

	values, err = s.driver.GetHashEngine().HVals(key)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)
}

func (s *testNemoHashSuite) TestHGetAll(c *C) {
	key := []byte("TestHGetAll")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	values, err := s.driver.GetHashEngine().HGetAll(key)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 0)

	s.driver.GetHashEngine().HSet(key, field1, value1)
	s.driver.GetHashEngine().HSet(key, field2, value2)

	values, err = s.driver.GetHashEngine().HGetAll(key)
	c.Assert(err, IsNil)
	c.Assert(len(values), Equals, 2)
}

func (s *testNemoHashSuite) TestHLen(c *C) {
	key := []byte("TestHLen")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	n, err := s.driver.GetHashEngine().HLen(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	s.driver.GetHashEngine().HSet(key, field1, value1)
	s.driver.GetHashEngine().HSet(key, field2, value2)

	n, err = s.driver.GetHashEngine().HLen(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))
}

func (s *testNemoHashSuite) TestHMGet(c *C) {
	key := []byte("TestHMGet")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	values, errs := s.driver.GetHashEngine().HMGet(key, field1, field2)
	c.Assert(len(errs), Equals, 0)
	c.Assert(len(values), Equals, 2)
	for i := 0; i < 2; i++ {
		c.Assert(len(values[i]), Equals, 0)
	}

	s.driver.GetHashEngine().HSet(key, field1, value1)
	s.driver.GetHashEngine().HSet(key, field2, value2)

	values, errs = s.driver.GetHashEngine().HMGet(key, field1, field2)
	c.Assert(len(errs), Equals, 0)
	c.Assert(len(values), Equals, 2)
}

func (s *testNemoHashSuite) TestHMSet(c *C) {
	key := []byte("TestHMSet")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	err := s.driver.GetHashEngine().HMSet(key, [][]byte{field1, field2}, [][]byte{value1, value2})
	c.Assert(err, IsNil)

	v, err := s.driver.GetHashEngine().HGet(key, field1)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(value1))

	v, err = s.driver.GetHashEngine().HGet(key, field2)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(value2))
}

func (s *testNemoHashSuite) TestHSetNX(c *C) {
	key := []byte("TestHSetNX")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")
	value2 := []byte("v2")

	s.driver.GetHashEngine().HSet(key, field1, value1)

	n, err := s.driver.GetHashEngine().HSetNX(key, field1, value1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))

	n, err = s.driver.GetHashEngine().HSetNX(key, field2, value2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))
}

func (s *testNemoHashSuite) TestHStrLen(c *C) {
	key := []byte("TestHStrLen")
	field1 := []byte("f1")
	value1 := []byte("v1")
	field2 := []byte("f2")

	s.driver.GetHashEngine().HSet(key, field1, value1)

	n, err := s.driver.GetHashEngine().HStrLen(key, field1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(len(value1)))

	n, err = s.driver.GetHashEngine().HStrLen(key, field2)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(0))
}

func (s *testNemoHashSuite) TestHIncrBy(c *C) {
	key := []byte("TestHIncrBy")
	field1 := []byte("f1")

	n, err := s.driver.GetHashEngine().HIncrBy(key, field1, 1)
	c.Assert(err, IsNil)
	c.Assert(string(n), Equals, "1")

	n, err = s.driver.GetHashEngine().HIncrBy(key, field1, 1)
	c.Assert(err, IsNil)
	c.Assert(string(n), Equals, "2")
}
