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

package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/storage"
	. "github.com/pingcap/check"
)

type testUtilSuite struct {
}

func (s *testUtilSuite) SetUpSuite(c *C) {

}

func (s *testUtilSuite) TearDownSuite(c *C) {

}

func (s *testUtilSuite) TestIsEpochStale(c *C) {
	e1 := createTestEpoch(0, 0)
	e2 := createTestEpoch(0, 1)
	e3 := createTestEpoch(1, 0)
	e4 := createTestEpoch(1, 1)

	c.Assert(isEpochStale(e1, e2), IsTrue)
	c.Assert(isEpochStale(e1, e3), IsTrue)
	c.Assert(isEpochStale(e1, e4), IsTrue)
}

func (s *testUtilSuite) TestSaveFirstCell(c *C) {
	peer := metapb.Peer{
		ID:      1,
		StoreID: 100,
	}

	cell := metapb.Cell{
		ID:    2,
		Peers: []*metapb.Peer{&peer},
	}

	driver := storage.NewMemoryDriver()
	err := SaveFirstCell(driver, cell)
	c.Assert(err, IsNil)

	count := 0
	err = driver.GetEngine().Scan(cellMetaMinKey, cellMetaMaxKey, func(key, value []byte) (bool, error) {
		cellID, suffix, err := decodeCellMetaKey(key)
		if err != nil {
			return false, err
		}

		c.Assert(cellID, Equals, cell.ID)
		count++

		if suffix != cellStateSuffix {
			return true, nil
		}

		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(count, Equals, 1)
}

func createTestEpoch(confVer, cellVer uint64) metapb.CellEpoch {
	return metapb.CellEpoch{
		ConfVer: confVer,
		CellVer: cellVer,
	}
}

func createTestPeer(id, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		ID:      id,
		StoreID: storeID,
	}
}

func createTestCell(id uint64, peers []*metapb.Peer) *metapb.Cell {
	return &metapb.Cell{
		ID:    id,
		Peers: peers,
	}
}
