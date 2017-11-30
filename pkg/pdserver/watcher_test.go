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

package pdserver

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	. "github.com/pingcap/check"
)

type testWatcherSuite struct {
}

func (s *testWatcherSuite) SetUpSuite(c *C) {

}

func (s *testWatcherSuite) TearDownSuite(c *C) {

}

func (s *testWatcherSuite) TestAddWatcher(c *C) {
	addr := "a"
	watcher := pdpb.Watcher{
		Addr:      addr,
		EventFlag: pd.EventFlagAll,
	}

	wn := newWatcherNotifier(time.Millisecond * 100)
	wn.addWatcher(watcher)
	c.Assert(len(wn.watchers) == 1, IsTrue)
	c.Assert(wn.watchers[addr].state == ready, IsTrue)
	time.Sleep(time.Millisecond * 150)
	c.Assert(len(wn.watchers) == 1, IsTrue)
	c.Assert(wn.watchers[addr].state == paused, IsTrue)

	c.Assert(wn.watcherHeartbeat(addr, 0), IsTrue)
	c.Assert(len(wn.watchers) == 1, IsTrue)
	c.Assert(wn.watchers[addr].state == ready, IsTrue)

	time.Sleep(time.Millisecond * 80)
	wn.watcherHeartbeat(addr, 0)
	c.Assert(len(wn.watchers) == 1, IsTrue)
	c.Assert(wn.watchers[addr].state == ready, IsTrue)
}

func (s *testWatcherSuite) TestRemoveWatcher(c *C) {
	addr := "a"
	watcher := pdpb.Watcher{
		Addr:      addr,
		EventFlag: pd.EventFlagAll,
	}
	wn := newWatcherNotifier(time.Millisecond * 100)
	wn.addWatcher(watcher)
	wn.removeWatcher(addr)
	c.Assert(len(wn.watchers) == 0, IsTrue)

	time.Sleep(time.Millisecond * 150)
	c.Assert(len(wn.watchers) == 0, IsTrue)

	c.Assert(wn.watcherHeartbeat(addr, 0), IsFalse)
	c.Assert(len(wn.watchers) == 0, IsTrue)
}

func (s *testWatcherSuite) TestNotify(c *C) {
	addr := "a"
	watcher := pdpb.Watcher{
		Addr:      addr,
		EventFlag: pd.EventFlagAll,
	}
	wn := newWatcherNotifier(time.Second * 100)
	wn.addWatcher(watcher)
	c.Assert(wn.watchers[addr].state == ready, IsTrue)
	wn.notify(&pdpb.WatchEvent{
		Event: pd.EventCellCreated,
		CellEvent: &pdpb.CellEvent{
			Range: new(pdpb.Range),
		},
	})
	c.Assert(wn.watchers[addr].q.GetMaxOffset() == 0, IsTrue)
	wn.notify(&pdpb.WatchEvent{
		Event: pd.EventCellCreated,
		CellEvent: &pdpb.CellEvent{
			Range: new(pdpb.Range),
		},
	})
	c.Assert(wn.watchers[addr].q.GetMaxOffset() == 1, IsTrue)

	// pause
	wn.pause(addr, false)
	time.Sleep(time.Millisecond * 150)
	c.Assert(wn.watchers[addr].state == paused, IsTrue)

	// resume
	wn.watcherHeartbeat(addr, 0)
	c.Assert(wn.watchers[addr].state == ready, IsTrue)
	wn.notify(&pdpb.WatchEvent{
		Event: pd.EventCellCreated,
		CellEvent: &pdpb.CellEvent{
			Range: new(pdpb.Range),
		},
	})
	wn.notify(&pdpb.WatchEvent{
		Event: pd.EventCellCreated,
		CellEvent: &pdpb.CellEvent{
			Range: new(pdpb.Range),
		},
	})
	wn.notify(&pdpb.WatchEvent{
		Event: pd.EventCellCreated,
		CellEvent: &pdpb.CellEvent{
			Range: new(pdpb.Range),
		},
	})
	c.Assert(wn.watchers[addr].q.GetMaxOffset() == 2, IsTrue)
}

func newTestNotify(offset uint64, addr string) *notify {
	return &notify{
		offset:  offset,
		watcher: addr,
	}
}
