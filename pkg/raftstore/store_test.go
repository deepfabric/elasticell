package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	. "github.com/pingcap/check"
)

type storeTestSuite struct {
}

func (s *storeTestSuite) SetUpSuite(c *C) {

}

func (s *storeTestSuite) TearDownSuite(c *C) {

}

func (s *storeTestSuite) TestGetTargetCell(c *C) {
	store := new(Store)
	store.keyRanges = util.NewCellTree()
	store.keyConvertFun = util.NoConvert
	store.replicatesMap = newCellPeersMap()

	store.keyRanges.Update(metapb.Cell{
		ID:    1,
		Start: []byte{1},
		End:   []byte{10},
	})
	store.keyRanges.Update(metapb.Cell{
		ID:    2,
		Start: []byte{10},
	})
	store.replicatesMap.put(1, &PeerReplicate{
		cellID: 1,
	})
	store.replicatesMap.put(2, &PeerReplicate{
		cellID: 2,
	})

	pr, err := store.getTargetCell([]byte{1})
	c.Assert(err, IsNil)
	c.Assert(pr.cellID == 1, IsTrue)

	pr, err = store.getTargetCell([]byte{9})
	c.Assert(err, IsNil)
	c.Assert(pr.cellID == 1, IsTrue)

	pr, err = store.getTargetCell([]byte{10})
	c.Assert(err, IsNil)
	c.Assert(pr.cellID == 2, IsTrue)

	pr, err = store.getTargetCell([]byte{0xff})
	c.Assert(err, IsNil)
	c.Assert(pr.cellID == 2, IsTrue)

	_, err = store.getTargetCell([]byte{0})
	c.Assert(err, NotNil)
}

func (s *storeTestSuite) TestCleanup(c *C) {
	store := new(Store)
	store.engine = storage.NewMemoryDriver()
	store.keyRanges = util.NewCellTree()
	store.keyRanges.Update(metapb.Cell{
		ID:    1,
		Start: []byte{1},
		End:   []byte{10},
		Peers: []*metapb.Peer{newTestPeer(1, 1)},
	})
	store.keyRanges.Update(metapb.Cell{
		ID:    2,
		Start: []byte{11},
		End:   []byte{12},
		Peers: []*metapb.Peer{newTestPeer(2, 1)},
	})

	value := []byte("value")
	c.Assert(store.engine.GetKVEngine().Set(getDataKey([]byte{0}), value), IsNil)
	c.Assert(store.engine.GetKVEngine().Set(getDataKey([]byte{1}), value), IsNil)
	c.Assert(store.engine.GetKVEngine().Set(getDataKey([]byte{9}), value), IsNil)
	c.Assert(store.engine.GetKVEngine().Set(getDataKey([]byte{10}), value), IsNil)
	c.Assert(store.engine.GetKVEngine().Set(getDataKey([]byte{11}), value), IsNil)
	c.Assert(store.engine.GetKVEngine().Set(getDataKey([]byte{12}), value), IsNil)

	store.cleanup()

	value, err := store.engine.GetKVEngine().Get(getDataKey([]byte{0}))
	c.Assert(err, IsNil)
	c.Assert(len(value) == 0, IsTrue)

	value, err = store.engine.GetKVEngine().Get(getDataKey([]byte{1}))
	c.Assert(err, IsNil)
	c.Assert(len(value) > 0, IsTrue)

	value, err = store.engine.GetKVEngine().Get(getDataKey([]byte{9}))
	c.Assert(err, IsNil)
	c.Assert(len(value) > 0, IsTrue)

	value, err = store.engine.GetKVEngine().Get(getDataKey([]byte{10}))
	c.Assert(err, IsNil)
	c.Assert(len(value) == 0, IsTrue)

	value, err = store.engine.GetKVEngine().Get(getDataKey([]byte{11}))
	c.Assert(err, IsNil)
	c.Assert(len(value) > 0, IsTrue)

	value, err = store.engine.GetKVEngine().Get(getDataKey([]byte{12}))
	c.Assert(err, IsNil)
	c.Assert(len(value) == 0, IsTrue)
}

func (s *storeTestSuite) TestClearMeta(c *C) {
	store := new(Store)
	store.engine = storage.NewMemoryDriver()
	store.keyRanges = util.NewCellTree()
	c1 := metapb.Cell{
		ID:    1,
		Start: []byte{1},
		End:   []byte{10},
		Peers: []*metapb.Peer{newTestPeer(1, 1)},
	}
	store.keyRanges.Update(c1)
	c2 := metapb.Cell{
		ID:    2,
		Start: []byte{11},
		End:   []byte{12},
		Peers: []*metapb.Peer{newTestPeer(2, 1)},
	}
	store.keyRanges.Update(c2)

	c.Assert(SaveCell(store.engine, c1), IsNil)
	c.Assert(SaveCell(store.engine, c2), IsNil)
	wb := store.engine.NewWriteBatch()
	store.clearMeta(c1.ID, wb)
	store.clearMeta(c2.ID, wb)
	store.engine.Write(wb, false)

	data, err := store.engine.GetEngine().Get(getCellStateKey(c1.ID))
	c.Assert(err, IsNil)
	c.Assert(len(data) == 0, IsTrue)

	data, err = store.engine.GetEngine().Get(getRaftStateKey(c1.ID))
	c.Assert(err, IsNil)
	c.Assert(len(data) == 0, IsTrue)

	data, err = store.engine.GetEngine().Get(getApplyStateKey(c1.ID))
	c.Assert(err, IsNil)
	c.Assert(len(data) == 0, IsTrue)

	data, err = store.engine.GetEngine().Get(getCellStateKey(c2.ID))
	c.Assert(err, IsNil)
	c.Assert(len(data) == 0, IsTrue)

	data, err = store.engine.GetEngine().Get(getRaftStateKey(c2.ID))
	c.Assert(err, IsNil)
	c.Assert(len(data) == 0, IsTrue)

	data, err = store.engine.GetEngine().Get(getApplyStateKey(c2.ID))
	c.Assert(err, IsNil)
	c.Assert(len(data) == 0, IsTrue)
}
