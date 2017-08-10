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
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/util"
)

// CellInfo The cell info
type CellInfo struct {
	Meta         metapb.Cell
	LeaderPeer   *metapb.Peer
	DownPeers    []pdpb.PeerStats
	PendingPeers []metapb.Peer
}

func newCellInfo(cell metapb.Cell, leader *metapb.Peer) *CellInfo {
	return &CellInfo{
		Meta:       cell,
		LeaderPeer: leader,
	}
}

type cellCache struct {
	sync.RWMutex

	tree      *util.CellTree
	cells     map[uint64]*CellInfo            // cellID -> cellRuntimeInfo
	leaders   map[uint64]map[uint64]*CellInfo // storeID -> cellID -> cellRuntimeInfo
	followers map[uint64]map[uint64]*CellInfo // storeID -> cellID -> cellRuntimeInfo
}

func newCellCache() *cellCache {
	cc := new(cellCache)
	cc.tree = util.NewCellTree()
	cc.cells = make(map[uint64]*CellInfo)
	cc.leaders = make(map[uint64]map[uint64]*CellInfo)
	cc.followers = make(map[uint64]map[uint64]*CellInfo)

	return cc
}

func (cc *cellCache) createAndAdd(cell metapb.Cell) {
	cc.Lock()
	defer cc.Unlock()

	cc.addOrUpdateWithoutLock(newCellInfo(cell, nil))
}

func (cc *cellCache) addOrUpdate(cellInfo *CellInfo) {
	cc.Lock()
	defer cc.Unlock()

	cc.addOrUpdateWithoutLock(cellInfo)
}

func (cc *cellCache) addOrUpdateWithoutLock(origin *CellInfo) {
	if cell, ok := cc.cells[origin.getID()]; ok {
		cc.removeCell(cell)
	}

	// Add to tree and regions.
	cc.tree.Update(origin.Meta)
	cc.cells[origin.getID()] = origin

	if origin.LeaderPeer == nil || origin.LeaderPeer.ID == pd.ZeroID {
		return
	}

	// Add to leaders and followers.
	for _, peer := range origin.getPeers() {
		storeID := peer.StoreID
		if peer.ID == origin.LeaderPeer.ID {
			// Add leader peer to leaders.
			store, ok := cc.leaders[storeID]
			if !ok {
				store = make(map[uint64]*CellInfo)
				cc.leaders[storeID] = store
			}
			store[origin.getID()] = origin
		} else {
			// Add follower peer to followers.
			store, ok := cc.followers[storeID]
			if !ok {
				store = make(map[uint64]*CellInfo)
				cc.followers[storeID] = store
			}
			store[origin.getID()] = origin
		}
	}
}

func (cc *cellCache) removeCell(origin *CellInfo) {
	// Remove from tree and cells.
	cc.tree.Remove(origin.Meta)
	delete(cc.cells, origin.getID())

	// Remove from leaders and followers.
	for _, peer := range origin.getPeers() {
		delete(cc.leaders[peer.StoreID], origin.getID())
		delete(cc.followers[peer.StoreID], origin.getID())
	}
}

func (cc *cellCache) searchCell(startKey []byte) *CellInfo {
	cc.RLock()
	defer cc.RUnlock()

	cell := cc.tree.Search(startKey)
	if cell.ID == pd.ZeroID {
		return nil
	}

	return cc.cells[cell.ID]
}

func (cc *cellCache) foreach(fn func(*CellInfo) (bool, error)) error {
	cc.RLock()
	defer cc.RUnlock()

	for _, c := range cc.cells {
		next, err := fn(c)
		if err != nil {
			return err
		}

		if !next {
			break
		}
	}

	return nil
}

func (cc *cellCache) getCells() []*CellInfo {
	cc.RLock()
	defer cc.RUnlock()

	cells := make([]*CellInfo, 0, len(cc.cells))
	for _, cell := range cc.cells {
		cells = append(cells, cell.clone())
	}
	return cells
}

func (cc *cellCache) getCell(id uint64) *CellInfo {
	cc.RLock()
	defer cc.RUnlock()

	c := cc.cells[id]
	if c != nil {
		return c.clone()
	}

	return c
}

func (cc *cellCache) randFollowerCell(storeID uint64) *CellInfo {
	cc.RLock()
	defer cc.RUnlock()

	return randCell(cc.followers[storeID])
}

func (cc *cellCache) randLeaderCell(storeID uint64) *CellInfo {
	cc.RLock()
	defer cc.RUnlock()

	return randCell(cc.leaders[storeID])
}

func (cc *cellCache) getStoreLeaderCount(storeID uint64) int {
	return len(cc.leaders[storeID])
}

func (cc *CellInfo) getFollowers() map[uint64]*metapb.Peer {
	peers := cc.Meta.Peers
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if cc.LeaderPeer == nil || cc.LeaderPeer.ID != peer.ID {
			followers[peer.StoreID] = peer
		}
	}

	return followers
}

func (cc *CellInfo) getStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range cc.Meta.Peers {
		if peer.StoreID == storeID {
			return peer
		}
	}
	return nil
}

func (cc *CellInfo) removeStorePeer(storeID uint64) {
	var peers []*metapb.Peer
	for _, peer := range cc.getPeers() {
		if peer.StoreID != storeID {
			peers = append(peers, peer)
		}
	}

	cc.Meta.Peers = peers
}

func (cc *CellInfo) getPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range cc.PendingPeers {
		if peer.ID == peerID {
			return &peer
		}
	}
	return nil
}

func (cc *CellInfo) getPeer(peerID uint64) *metapb.Peer {
	for _, peer := range cc.Meta.Peers {
		if peer.ID == peerID {
			return peer
		}
	}

	return nil
}

func (cc *CellInfo) getID() uint64 {
	return cc.Meta.ID
}

func (cc *CellInfo) getPeers() []*metapb.Peer {
	return cc.Meta.Peers
}

func (cc *CellInfo) getStoreIDs() map[uint64]struct{} {
	peers := cc.getPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.StoreID] = struct{}{}
	}
	return stores
}

type idCache struct {
	*expireCellCache
}

func newIDCache(interval, ttl time.Duration) *idCache {
	return &idCache{
		expireCellCache: neweExpireCellCache(interval, ttl),
	}
}

func (c *idCache) set(id uint64) {
	c.expireCellCache.set(id, nil)
}

func (c *idCache) get(id uint64) bool {
	_, ok := c.expireCellCache.get(id)
	return ok
}

type cacheItem struct {
	key    uint64
	value  interface{}
	expire time.Time
}

// expireCellCache is an expired region cache.
type expireCellCache struct {
	sync.RWMutex

	items      map[uint64]cacheItem
	ttl        time.Duration
	gcInterval time.Duration
}

// newExpireCellCache returns a new expired region cache.
func neweExpireCellCache(gcInterval time.Duration, ttl time.Duration) *expireCellCache {
	c := &expireCellCache{
		items:      make(map[uint64]cacheItem),
		ttl:        ttl,
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

func (c *expireCellCache) get(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expire.Before(time.Now()) {
		return nil, false
	}

	return item.value, true
}

func (c *expireCellCache) set(key uint64, value interface{}) {
	c.setWithTTL(key, value, c.ttl)
}

func (c *expireCellCache) setWithTTL(key uint64, value interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = cacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

func (c *expireCellCache) delete(key uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

func (c *expireCellCache) count() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *expireCellCache) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := 0
			now := time.Now()
			c.Lock()
			for key := range c.items {
				if value, ok := c.items[key]; ok {
					if value.expire.Before(now) {
						count++
						delete(c.items, key)
					}
				}
			}
			c.Unlock()

			log.Debugf("GC %d items", count)
		}
	}
}
