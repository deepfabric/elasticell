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

type testNemoWBSuite struct {
	driver Driver
}

func (s *testNemoWBSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver(&NemoCfg{
		DataPath: "/tmp/nemo-wb",
	})
	c.Assert(err, IsNil)
}

func (s *testNemoWBSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-wb")
}

func (s *testNemoWBSuite) TestWB(c *C) {
	key1 := []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")

	wb := s.driver.NewWriteBatch()
	s.set(wb, key1, c)
	s.set(wb, key2, c)
	s.set(wb, key3, c)
	s.delete(wb, key1, c)

	err := s.driver.Write(wb, false)
	c.Assert(err, IsNil)

	s.checkNotExists(key1, c)
	s.checkExists(key2, c)
	s.checkExists(key3, c)
}

func (s *testNemoWBSuite) set(wb WriteBatch, key []byte, c *C) {
	value := []byte("kv-value")
	err := wb.Set(key, value)
	c.Assert(err, IsNil)
}

func (s *testNemoWBSuite) delete(wb WriteBatch, key []byte, c *C) {
	err := wb.Delete(key)
	c.Assert(err, IsNil)
}

func (s *testNemoWBSuite) checkExists(key []byte, c *C) {
	v, err := s.driver.GetEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v) > 0, IsTrue)
}

func (s *testNemoWBSuite) checkNotExists(key []byte, c *C) {
	v, err := s.driver.GetEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v) == 0, IsTrue)
}
