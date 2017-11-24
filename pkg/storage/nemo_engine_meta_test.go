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

type testNemoMetaSuite struct {
	driver Driver
}

func (s *testNemoMetaSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver(&NemoCfg{
		DataPath: "/tmp/nemo-meta",
	})
	c.Assert(err, IsNil)
}

func (s *testNemoMetaSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-meta")
}

func (s *testNemoMetaSuite) TestSet(c *C) {
	key := []byte("TestSet")
	value := []byte("value1")

	err := s.driver.GetEngine().Set(key, value)
	c.Assert(err, IsNil)
}

func (s *testNemoMetaSuite) TestGet(c *C) {
	key := []byte("TestGet")
	value := []byte("value2")

	v, err := s.driver.GetEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	err = s.driver.GetEngine().Set(key, value)
	c.Assert(err, IsNil)

	v, err = s.driver.GetEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(string(v), Equals, string(value))
}

func (s *testNemoMetaSuite) TestDelete(c *C) {
	key := []byte("TestDelete")
	value := []byte("value1")

	err := s.driver.GetEngine().Set(key, value)
	c.Assert(err, IsNil)

	err = s.driver.GetEngine().Delete(key)
	c.Assert(err, IsNil)

	v, err := s.driver.GetEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

}

func (s *testNemoMetaSuite) TestRangeDelete(c *C) {
	key1 := []byte("TestRangeDelete-a")
	key2 := []byte("TestRangeDelete-b")
	key3 := []byte("TestRangeDelete-c")
	key4 := []byte("TestRangeDelete-d")
	value := []byte("value1")

	s.driver.GetEngine().Set(key1, value)
	s.driver.GetEngine().Set(key2, value)
	s.driver.GetEngine().Set(key3, value)
	s.driver.GetEngine().Set(key4, value)

	err := s.driver.GetEngine().RangeDelete(key1, []byte("TestRangeDelete-e"))
	c.Assert(err, IsNil)

	v, err := s.driver.GetEngine().Get(key1)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	v, err = s.driver.GetEngine().Get(key2)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	v, err = s.driver.GetEngine().Get(key3)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)

	v, err = s.driver.GetEngine().Get(key4)
	c.Assert(err, IsNil)
	c.Assert(len(v), Equals, 0)
}

func (s *testNemoMetaSuite) TestScan(c *C) {
	key1 := []byte("TestScan-a")
	key2 := []byte("TestScan-b")
	key3 := []byte("TestScan-c")
	key4 := []byte("TestScan-d")
	value := []byte("value1")

	s.driver.GetEngine().Set(key1, value)
	s.driver.GetEngine().Set(key2, value)
	s.driver.GetEngine().Set(key3, value)
	s.driver.GetEngine().Set(key4, value)

	cnt := 0
	err := s.driver.GetEngine().Scan(key1, []byte("TestScan-e"), func(key, value []byte) (bool, error) {
		cnt++
		return true, nil
	}, false)

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 4)
}

func (s *testNemoMetaSuite) TestPooledScan(c *C) {
	key1 := []byte("TestScan-a")
	key2 := []byte("TestScan-b")
	key3 := []byte("TestScan-c")
	key4 := []byte("TestScan-d")
	value := []byte("value1")

	s.driver.GetEngine().Set(key1, value)
	s.driver.GetEngine().Set(key2, value)
	s.driver.GetEngine().Set(key3, value)
	s.driver.GetEngine().Set(key4, value)

	cnt := 0
	err := s.driver.GetEngine().Scan(key1, []byte("TestScan-e"), func(key, value []byte) (bool, error) {
		cnt++
		return true, nil
	}, true)

	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 4)
}

func (s *testNemoMetaSuite) TestSeek(c *C) {
	key1 := []byte("TestSeek-a")
	value1 := []byte("value1")
	key2 := []byte("TestSeek-c")
	value2 := []byte("value1")
	key3 := []byte("TestSeek-d")
	value3 := []byte("value1")
	key4 := []byte("TestSeek-e")
	value4 := []byte("value1")

	key5 := []byte("TestSeek-f")
	key1_2 := []byte("TestSeek-b")

	s.driver.GetEngine().Set(key1, value1)
	s.driver.GetEngine().Set(key2, value2)
	s.driver.GetEngine().Set(key3, value3)
	s.driver.GetEngine().Set(key4, value4)

	k, v, err := s.driver.GetEngine().Seek(key1)
	c.Assert(err, IsNil)
	c.Assert(string(k), Equals, string(key1))
	c.Assert(string(v), Equals, string(value1))

	k, v, err = s.driver.GetEngine().Seek(key1_2)
	c.Assert(err, IsNil)
	c.Assert(string(k), Equals, string(key2))
	c.Assert(string(v), Equals, string(value2))

	k, v, err = s.driver.GetEngine().Seek(key5)
	c.Assert(err, IsNil)
	c.Assert(len(k), Equals, 0)
	c.Assert(len(v), Equals, 0)
}
