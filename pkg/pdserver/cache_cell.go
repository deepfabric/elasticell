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
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/gogo/protobuf/proto"
)

func newCellRuntimeInfo(cell metapb.Cell, leader *metapb.Peer) *cellRuntimeInfo {
	return &cellRuntimeInfo{
		cell:   cell,
		leader: leader,
	}
}

func newCellCache() *cellCache {
	cc := new(cellCache)
	cc.tree = util.NewCellTree()
	cc.cells = make(map[uint64]*cellRuntimeInfo)
	cc.leaders = make(map[uint64]map[uint64]*cellRuntimeInfo)
	cc.followers = make(map[uint64]map[uint64]*cellRuntimeInfo)

	return cc
}

type cellRuntimeInfo struct {
	cell         metapb.Cell
	leader       *metapb.Peer
	downPeers    []pdpb.PeerStats
	pendingPeers []metapb.Peer
}

type cellCache struct {
	tree      *util.CellTree
	cells     map[uint64]*cellRuntimeInfo            // cellID -> cellRuntimeInfo
	leaders   map[uint64]map[uint64]*cellRuntimeInfo // storeID -> cellID -> cellRuntimeInfo
	followers map[uint64]map[uint64]*cellRuntimeInfo // storeID -> cellID -> cellRuntimeInfo
}

func (cc *cellCache) getStoreLeaderCount(storeID uint64) int {
	return len(cc.leaders[storeID])
}

func (cc *cellCache) randFollowerCell(storeID uint64) *cellRuntimeInfo {
	return randCell(cc.followers[storeID])
}

func (cc *cellCache) randLeaderCell(storeID uint64) *cellRuntimeInfo {
	return randCell(cc.leaders[storeID])
}

func (cc *cellRuntimeInfo) clone() *cellRuntimeInfo {
	downPeers := make([]pdpb.PeerStats, 0, len(cc.downPeers))
	for _, peer := range cc.downPeers {
		p := proto.Clone(&peer).(*pdpb.PeerStats)
		downPeers = append(downPeers, *p)
	}

	pendingPeers := make([]metapb.Peer, 0, len(cc.pendingPeers))
	for _, peer := range cc.pendingPeers {
		p := proto.Clone(&peer).(*metapb.Peer)
		pendingPeers = append(pendingPeers, *p)
	}

	return &cellRuntimeInfo{
		cell:         *(proto.Clone(&cc.cell).(*metapb.Cell)),
		leader:       proto.Clone(cc.leader).(*metapb.Peer),
		downPeers:    downPeers,
		pendingPeers: pendingPeers,
	}
}

func (cc *cellRuntimeInfo) getFollowers() map[uint64]*metapb.Peer {
	peers := cc.cell.Peers
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if cc.leader == nil || cc.leader.ID != peer.ID {
			followers[peer.StoreID] = peer
		}
	}

	return followers
}

func (cc *cellRuntimeInfo) getStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range cc.cell.Peers {
		if peer.StoreID == storeID {
			return peer
		}
	}
	return nil
}

func (cc *cellRuntimeInfo) getPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range cc.pendingPeers {
		if peer.ID == peerID {
			return &peer
		}
	}
	return nil
}

func (cc *cellRuntimeInfo) getPeer(peerID uint64) *metapb.Peer {
	for _, peer := range cc.cell.Peers {
		if peer.ID == peerID {
			return peer
		}
	}

	return nil
}

func (cc *cellRuntimeInfo) getID() uint64 {
	return cc.cell.ID
}

func (cc *cellRuntimeInfo) getPeers() []*metapb.Peer {
	return cc.cell.Peers
}

func (cc *cellRuntimeInfo) getStoreIDs() map[uint64]struct{} {
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
