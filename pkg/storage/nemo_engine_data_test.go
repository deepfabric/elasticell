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
	"fmt"
	"os"

	"github.com/deepfabric/elasticell/pkg/util"
	. "github.com/pingcap/check"
)

const (
	snapPath       = "/tmp/nemo-snap"
	snapCreatePath = "/tmp/nemo-create-snap"
	applyPath      = "/tmp/nemo-apply"
)

type testNemoDataSuite struct {
	driver Driver
}

func (s *testNemoDataSuite) SetUpSuite(c *C) {
	var err error
	s.driver, err = NewNemoDriver("/tmp/nemo-data")
	c.Assert(err, IsNil)
}

func (s *testNemoDataSuite) TearDownSuite(c *C) {
	os.RemoveAll("/tmp/nemo-data")
	os.RemoveAll(snapPath)
	os.RemoveAll(snapCreatePath)
	os.RemoveAll(applyPath)
}

func (s *testNemoDataSuite) TestRangeDelete(c *C) {
	kvKey := []byte("TestRangeDelete-a")
	s.addKV(kvKey, c)

	listKey := []byte("TestRangeDelete-b")
	s.addList(listKey, c)

	setKey := []byte("TestRangeDelete-c")
	s.addSet(setKey, c)

	zsetKey := []byte("TestRangeDelete-d")
	s.addZSet(zsetKey, c)

	hashKey := []byte("TestRangeDelete-e")
	s.addHash(hashKey, c)

	err := s.driver.GetDataEngine().RangeDelete(kvKey, []byte("TestRangeDelete-f"))
	c.Assert(err, IsNil)

	s.checkKV(kvKey, c)
	s.checkList(listKey, c)
	s.checkSet(setKey, c)
	s.checkZSet(zsetKey, c)
	s.checkHash(hashKey, c)
}

func (s *testNemoDataSuite) TestScanSize(c *C) {
	kvKey := []byte("TestRangeDelete-a")
	s.addKV(kvKey, c)

	listKey := []byte("TestRangeDelete-b")
	s.addList(listKey, c)

	setKey := []byte("TestRangeDelete-c")
	s.addSet(setKey, c)

	zsetKey := []byte("TestRangeDelete-d")
	s.addZSet(zsetKey, c)

	hashKey := []byte("TestRangeDelete-e")
	s.addHash(hashKey, c)

	total, key, err := s.driver.GetDataEngine().GetTargetSizeKey(kvKey, []byte("TestRangeDelete-f"), 1024*1024)

	c.Assert(err, IsNil)
	c.Assert(total > 0, IsTrue)
	c.Assert(len(key) == 0, IsTrue)
}

func (s *testNemoDataSuite) CreateSnapshot(c *C) {
	s.driver.GetDataEngine().RangeDelete([]byte(""), []byte{byte('z' + 1)})
	kvKey := []byte("CreateSnapshot-a")
	s.addKV(kvKey, c)

	listKey := []byte("CreateSnapshot-b")
	s.addList(listKey, c)

	setKey := []byte("CreateSnapshot-c")
	s.addSet(setKey, c)

	zsetKey := []byte("CreateSnapshot-d")
	s.addZSet(zsetKey, c)

	hashKey := []byte("CreateSnapshot-e")
	s.addHash(hashKey, c)

	err := s.driver.GetDataEngine().CreateSnapshot(snapCreatePath, kvKey, hashKey)
	c.Assert(err, IsNil)
}

func (s *testNemoDataSuite) TestApplySnapshot(c *C) {
	driver, err := NewNemoDriver(applyPath)
	c.Assert(err, IsNil)

	s.driver.GetDataEngine().RangeDelete([]byte(""), []byte{byte('z' + 1)})
	kvKey := []byte("CreateSnapshot-a")
	s.addKV(kvKey, c)

	listKey := []byte("CreateSnapshot-b")
	s.addList(listKey, c)

	setKey := []byte("CreateSnapshot-c")
	s.addSet(setKey, c)

	zsetKey := []byte("CreateSnapshot-d")
	s.addZSet(zsetKey, c)

	hashKey := []byte("CreateSnapshot-e")
	s.addHash(hashKey, c)

	err = s.driver.GetDataEngine().CreateSnapshot(snapPath, kvKey, []byte("CreateSnapshot-f"))
	c.Assert(err, IsNil)

	err = util.GZIP(snapPath)
	c.Assert(err, IsNil)
	os.RemoveAll(snapPath)
	err = util.UnGZIP(fmt.Sprintf("%s.gz", snapPath), "/tmp")
	c.Assert(err, IsNil)

	err = driver.GetDataEngine().ApplySnapshot(snapPath)
	c.Assert(err, IsNil)

	s.checkApplyKV(driver, kvKey, c)
	s.checkApplyList(driver, listKey, c)
	s.checkApplySet(driver, setKey, c)
	s.checkApplyZSet(driver, zsetKey, c)
	s.checkApplyHash(driver, hashKey, c)

	total, key, err := s.driver.GetDataEngine().GetTargetSizeKey(kvKey, []byte("TestRangeDelete-f"), 1024*1024)

	c.Assert(err, IsNil)
	c.Assert(total > 0, IsTrue)
	c.Assert(len(key) == 0, IsTrue)
}

func (s *testNemoDataSuite) addKV(key []byte, c *C) {
	value := []byte("kv-value")
	err := s.driver.GetKVEngine().Set(key, value)
	c.Assert(err, IsNil)
}

func (s *testNemoDataSuite) checkKV(key []byte, c *C) {
	v, err := s.driver.GetKVEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v) == 0, IsTrue)
}

func (s *testNemoDataSuite) checkApplyKV(driver Driver, key []byte, c *C) {
	v, err := driver.GetKVEngine().Get(key)
	c.Assert(err, IsNil)
	c.Assert(len(v) > 0, IsTrue)
}

func (s *testNemoDataSuite) addHash(key []byte, c *C) {
	field := []byte("hash-field")
	value := []byte("hash-field-value")
	count, err := s.driver.GetHashEngine().HSet(key, field, value)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))
}

func (s *testNemoDataSuite) checkHash(key []byte, c *C) {
	field := []byte("hash-field")
	v, err := s.driver.GetHashEngine().HGet(key, field)
	c.Assert(err, IsNil)
	c.Assert(len(v) == 0, IsTrue)
}

func (s *testNemoDataSuite) checkApplyHash(driver Driver, key []byte, c *C) {
	field := []byte("hash-field")
	v, err := driver.GetHashEngine().HGet(key, field)
	c.Assert(err, IsNil)
	c.Assert(len(v) > 0, IsTrue)
}

func (s *testNemoDataSuite) addSet(key []byte, c *C) {
	member := []byte("set-member")
	count, err := s.driver.GetSetEngine().SAdd(key, member)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))
}

func (s *testNemoDataSuite) checkSet(key []byte, c *C) {
	count, err := s.driver.GetSetEngine().SCard(key)
	c.Assert(err, IsNil)
	c.Assert(count == 0, IsTrue)
}

func (s *testNemoDataSuite) checkApplySet(driver Driver, key []byte, c *C) {
	count, err := driver.GetSetEngine().SCard(key)
	c.Assert(err, IsNil)
	c.Assert(count > 0, IsTrue)
}

func (s *testNemoDataSuite) addZSet(key []byte, c *C) {
	member := []byte("zset-member")
	score := 1.0
	count, err := s.driver.GetZSetEngine().ZAdd(key, score, member)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))
}

func (s *testNemoDataSuite) checkZSet(key []byte, c *C) {
	count, err := s.driver.GetZSetEngine().ZCard(key)
	c.Assert(err, IsNil)
	c.Assert(count == 0, IsTrue)
}

func (s *testNemoDataSuite) checkApplyZSet(driver Driver, key []byte, c *C) {
	count, err := driver.GetZSetEngine().ZCard(key)
	c.Assert(err, IsNil)
	c.Assert(count > 0, IsTrue)
}

func (s *testNemoDataSuite) addList(key []byte, c *C) {
	value := []byte("list-value")
	count, err := s.driver.GetListEngine().LPush(key, value)
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(1))
}

func (s *testNemoDataSuite) checkList(key []byte, c *C) {
	count, err := s.driver.GetListEngine().LLen(key)
	c.Assert(err, IsNil)
	c.Assert(count == 0, IsTrue)
}

func (s *testNemoDataSuite) checkApplyList(driver Driver, key []byte, c *C) {
	count, err := driver.GetListEngine().LLen(key)
	c.Assert(err, IsNil)
	c.Assert(count > 0, IsTrue)
}
