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
	s.driver, err = NewNemoDriver("/tmp/nemo-wb")
	c.Assert(err, IsNil)
}

func (s *testNemoWBSuite) TearDownSuite(c *C) {
	err := os.RemoveAll("/tmp/nemo-wb")
	c.Assert(err, IsNil)
}

func (s *testNemoWBSuite) TestDelete(c *C) {

}

func (s *testNemoWBSuite) TestSet(c *C) {

}
