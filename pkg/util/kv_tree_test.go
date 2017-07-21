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

package util

import (
	"errors"
	"testing"

	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testKVTreeSuite{})

func TestUtil(t *testing.T) {
	TestingT(t)
}

type testKVTreeSuite struct {
}

func (s *testKVTreeSuite) SetUpSuite(c *C) {

}

func (s *testKVTreeSuite) TearDownSuite(c *C) {

}

func (s *testKVTreeSuite) TestKVTreePutAndGet(c *C) {
	tree := NewKVTree()

	key := []byte("key1")
	value := []byte("value1")
	tree.Put(key, value)

	v := tree.Get(key)
	c.Assert(string(v), Equals, string(value))
}

func (s *testKVTreeSuite) TestKVTreeDelete(c *C) {
	tree := NewKVTree()

	key := []byte("key1")
	value := []byte("value1")

	c.Assert(tree.Delete(key), IsFalse)

	tree.Put(key, value)
	c.Assert(tree.Delete(key), IsTrue)

	v := tree.Get(key)
	c.Assert(v, IsNil)
}

func (s *testKVTreeSuite) TestKVTreeRangeDelete(c *C) {
	tree := NewKVTree()

	key1 := []byte("key1")
	value1 := []byte("value1")

	key2 := []byte("key2")
	value2 := []byte("value2")

	key3 := []byte("key3")
	value3 := []byte("value3")

	key4 := []byte("key4")
	value4 := []byte("value4")

	tree.Put(key1, value1)
	tree.Put(key2, value2)
	tree.Put(key3, value3)
	tree.Put(key4, value4)

	tree.RangeDelete(key1, key4)

	v := tree.Get(key1)
	c.Assert(v, IsNil)

	v = tree.Get(key2)
	c.Assert(v, IsNil)

	v = tree.Get(key3)
	c.Assert(v, IsNil)

	v = tree.Get(key4)
	c.Assert(v, NotNil)
}

func (s *testKVTreeSuite) TestKVTreeSeek(c *C) {
	tree := NewKVTree()

	key1 := []byte("key1")
	value1 := []byte("value1")

	key3 := []byte("key3")
	value3 := []byte("value3")

	key2 := []byte("key2")
	key4 := []byte("key4")

	tree.Put(key1, value1)
	tree.Put(key3, value3)

	k, v := tree.Seek(key1)
	c.Assert(string(k), Equals, string(key1))
	c.Assert(string(v), Equals, string(value1))

	k, v = tree.Seek(key2)
	c.Assert(string(k), Equals, string(key3))
	c.Assert(string(v), Equals, string(value3))

	k, v = tree.Seek(key3)
	c.Assert(string(k), Equals, string(key3))
	c.Assert(string(v), Equals, string(value3))

	k, v = tree.Seek(key4)
	c.Assert(k, IsNil)
	c.Assert(v, IsNil)
}

func (s *testKVTreeSuite) TestKVTreeScan(c *C) {
	tree := NewKVTree()

	key1 := []byte("key1")
	value1 := []byte("value1")

	key2 := []byte("key2")
	value2 := []byte("value2")

	key3 := []byte("key3")
	value3 := []byte("value3")

	key4 := []byte("key4")
	value4 := []byte("value4")

	tree.Put(key1, value1)
	tree.Put(key2, value2)
	tree.Put(key3, value3)
	tree.Put(key4, value4)

	cnt := 0
	err := tree.Scan(key1, key4, func(key, value []byte) (bool, error) {
		cnt++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 4)

	cnt = 0
	err = tree.Scan(key1, key4, func(key, value []byte) (bool, error) {
		if bytes.Compare(key, key2) == 0 {
			return false, nil
		}

		cnt++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, 1)

	cnt = 0
	err = tree.Scan(key1, key4, func(key, value []byte) (bool, error) {
		if bytes.Compare(key, key2) == 0 {
			return true, errors.New("err")
		}

		cnt++
		return true, nil
	})
	c.Assert(err, NotNil)
	c.Assert(cnt, Equals, 1)

	err = tree.Scan(key1, key4, func(key, value []byte) (bool, error) {
		tree.Delete(key)
		return true, nil
	})
	c.Assert(err, IsNil)

	v := tree.Get(key1)
	c.Assert(v, IsNil)

	v = tree.Get(key2)
	c.Assert(v, IsNil)

	v = tree.Get(key3)
	c.Assert(v, IsNil)

	v = tree.Get(key4)
	c.Assert(v, IsNil)
}
