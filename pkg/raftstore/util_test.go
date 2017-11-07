package raftstore

import (
	"bytes"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	. "github.com/pingcap/check"
)

type utilTestSuite struct {
}

func (s *utilTestSuite) SetUpSuite(c *C) {
}

func (s *utilTestSuite) TearDownSuite(c *C) {
}

func (s *utilTestSuite) TestIsEpochStale(c *C) {
	c.Assert(isEpochStale(newTestEpoch(1, 1), newTestEpoch(2, 1)), IsTrue)
	c.Assert(isEpochStale(newTestEpoch(1, 1), newTestEpoch(2, 2)), IsTrue)
	c.Assert(isEpochStale(newTestEpoch(1, 1), newTestEpoch(1, 1)), IsFalse)
	c.Assert(isEpochStale(newTestEpoch(2, 2), newTestEpoch(1, 1)), IsFalse)
	c.Assert(isEpochStale(newTestEpoch(2, 2), newTestEpoch(1, 2)), IsFalse)
	c.Assert(isEpochStale(newTestEpoch(2, 2), newTestEpoch(2, 1)), IsFalse)
}

func (s *utilTestSuite) TestFindPeer(c *C) {
	cell := &metapb.Cell{}
	cell.Peers = append(cell.Peers, newTestPeer(1, 1))
	cell.Peers = append(cell.Peers, newTestPeer(2, 2))
	cell.Peers = append(cell.Peers, newTestPeer(3, 3))

	found := findPeer(cell, 1)
	c.Assert(found, NotNil)
	c.Assert(found.ID == 1, IsTrue)

	found = findPeer(cell, 2)
	c.Assert(found, NotNil)
	c.Assert(found.ID == 2, IsTrue)

	found = findPeer(cell, 3)
	c.Assert(found, NotNil)
	c.Assert(found.ID == 3, IsTrue)

	found = findPeer(cell, 4)
	c.Assert(found, IsNil)
}

func (s *utilTestSuite) TestRemovePeer(c *C) {
	cell := &metapb.Cell{}
	cell.Peers = append(cell.Peers, newTestPeer(1, 1))
	cell.Peers = append(cell.Peers, newTestPeer(2, 2))
	cell.Peers = append(cell.Peers, newTestPeer(3, 3))

	removePeer(cell, 4)
	c.Assert(len(cell.Peers) == 3, IsTrue)

	removePeer(cell, 1)
	c.Assert(len(cell.Peers) == 2, IsTrue)

	removePeer(cell, 2)
	c.Assert(len(cell.Peers) == 1, IsTrue)

	removePeer(cell, 3)
	c.Assert(len(cell.Peers) == 0, IsTrue)
}

func (s *utilTestSuite) TestCheckKeyInCell(c *C) {
	cell := &metapb.Cell{
		Start: []byte{0},
		End:   []byte{10},
	}

	c.Assert(checkKeyInCell([]byte{0}, cell), IsNil)
	c.Assert(checkKeyInCell([]byte{9}, cell), IsNil)
	c.Assert(checkKeyInCell([]byte{10}, cell), NotNil)

	cell = &metapb.Cell{}
	c.Assert(checkKeyInCell([]byte{0}, cell), IsNil)
	c.Assert(checkKeyInCell([]byte{0xff}, cell), IsNil)
}

func (s *utilTestSuite) TestSaveCell(c *C) {
	d := storage.NewMemoryDriver()

	cell := metapb.Cell{
		ID:    1,
		Start: []byte{0},
		End:   []byte{0},
		Epoch: newTestEpoch(2, 3),
	}
	cell.Peers = append(cell.Peers, newTestPeer(1, 1))

	c.Assert(SaveCell(d, cell), IsNil)

	ls := new(mraft.CellLocalState)
	data, err := d.GetEngine().Get(getCellStateKey(cell.ID))
	c.Assert(err, IsNil)
	util.MustUnmarshal(ls, data)
	c.Assert(ls.State == mraft.Normal, IsTrue)
	c.Assert(ls.Cell.ID == cell.ID, IsTrue)
	c.Assert(bytes.Compare(ls.Cell.Start, cell.Start) == 0, IsTrue)
	c.Assert(bytes.Compare(ls.Cell.End, cell.End) == 0, IsTrue)
	c.Assert(len(ls.Cell.Peers) == 1, IsTrue)
	c.Assert(ls.Cell.Peers[0].ID == cell.Peers[0].ID, IsTrue)
	c.Assert(ls.Cell.Peers[0].StoreID == cell.Peers[0].StoreID, IsTrue)
	c.Assert(ls.Cell.Epoch.CellVer == cell.Epoch.CellVer, IsTrue)
	c.Assert(ls.Cell.Epoch.ConfVer == cell.Epoch.ConfVer, IsTrue)

	rs := new(mraft.RaftLocalState)
	data, err = d.GetEngine().Get(getRaftStateKey(cell.ID))
	c.Assert(err, IsNil)
	util.MustUnmarshal(rs, data)
	c.Assert(rs.LastIndex == raftInitLogIndex, IsTrue)
	c.Assert(rs.HardState.Commit == raftInitLogIndex, IsTrue)
	c.Assert(rs.HardState.Term == raftInitLogTerm, IsTrue)

	as := new(mraft.RaftApplyState)
	data, err = d.GetEngine().Get(getApplyStateKey(cell.ID))
	c.Assert(err, IsNil)
	util.MustUnmarshal(as, data)
	c.Assert(as.AppliedIndex == raftInitLogIndex, IsTrue)
	c.Assert(as.TruncatedState.Index == raftInitLogIndex, IsTrue)
	c.Assert(as.TruncatedState.Term == raftInitLogTerm, IsTrue)
}

func (s *utilTestSuite) TestRemovedPeers(c *C) {
	new := metapb.Cell{}
	new.Peers = append(new.Peers, newTestPeer(1, 100))
	new.Peers = append(new.Peers, newTestPeer(2, 101))
	new.Peers = append(new.Peers, newTestPeer(3, 102))

	old := metapb.Cell{}
	old.Peers = append(old.Peers, newTestPeer(1, 100))
	old.Peers = append(old.Peers, newTestPeer(2, 101))
	old.Peers = append(old.Peers, newTestPeer(4, 102))

	rms := removedPeers(new, old)
	c.Assert(len(rms) == 1, IsTrue)
	c.Assert(rms[0] == 4, IsTrue)

	new = metapb.Cell{}
	new.Peers = append(new.Peers, newTestPeer(1, 100))
	new.Peers = append(new.Peers, newTestPeer(2, 101))
	new.Peers = append(new.Peers, newTestPeer(3, 102))

	old = metapb.Cell{}
	old.Peers = append(old.Peers, newTestPeer(4, 100))
	old.Peers = append(old.Peers, newTestPeer(5, 101))
	old.Peers = append(old.Peers, newTestPeer(6, 102))

	rms = removedPeers(new, old)
	c.Assert(len(rms) == 3, IsTrue)
}

func newTestPeer(id, storeID uint64) *metapb.Peer {
	return &metapb.Peer{
		ID:      id,
		StoreID: storeID,
	}
}

func newTestEpoch(ConfVer, CellVer uint64) metapb.CellEpoch {
	return metapb.CellEpoch{
		ConfVer: ConfVer,
		CellVer: CellVer,
	}
}
