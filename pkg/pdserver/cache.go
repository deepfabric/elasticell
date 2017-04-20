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

	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pdserver/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

const (
	batchLimit = 10000
)

type clusterRuntime struct {
	cluster meta.Cluster
}

func newClusterRuntime(cluster meta.Cluster) *clusterRuntime {
	return &clusterRuntime{
		cluster: cluster,
	}
}

type cache struct {
	sync.RWMutex
	clusterID uint64
	cluster   *clusterRuntime
	sc        *storeCache
	cc        *cellCache
	store     *storage.Store
	allocator *idAllocator
}

type storeCache struct {
	stores map[uint64]*storeRuntime
}

type cellCache struct {
	tree      *util.CellTree
	cells     map[uint64]*cellRuntime            // cellID -> cellRuntime
	leaders   map[uint64]map[uint64]*cellRuntime // storeID -> cellID -> cellRuntime
	followers map[uint64]map[uint64]*cellRuntime // storeID -> cellID -> cellRuntime
}

func newCache(clusterID uint64, store *storage.Store, allocator *idAllocator) *cache {
	c := new(cache)
	c.clusterID = clusterID
	c.sc = newStoreCache()
	c.cc = newCellCache()
	c.store = store
	c.allocator = allocator

	return c
}

func newStoreCache() *storeCache {
	sc := new(storeCache)
	sc.stores = make(map[uint64]*storeRuntime)

	return sc
}

func newCellCache() *cellCache {
	cc := new(cellCache)
	cc.tree = util.NewCellTree()
	cc.cells = make(map[uint64]*cellRuntime)
	cc.leaders = make(map[uint64]map[uint64]*cellRuntime)
	cc.followers = make(map[uint64]map[uint64]*cellRuntime)

	return cc
}

func (cc *cellCache) getStoreLeaderCount(storeID uint64) int {
	return len(cc.leaders[storeID])
}

func (c *cache) allocPeer(storeID uint64) (meta.Peer, error) {
	peerID, err := c.allocator.newID()
	if err != nil {
		return meta.Peer{}, errors.Wrap(err, "")
	}

	peer := meta.Peer{
		ID:      peerID,
		StoreID: storeID,
	}
	return peer, nil
}

func (c *cache) getStores() []*storeRuntime {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*storeRuntime, 0, len(c.sc.stores))
	for _, store := range c.sc.stores {
		stores = append(stores, store.clone())
	}
	return stores
}

func (c *cache) getCellStores(cell *cellRuntime) []*storeRuntime {
	c.RLock()
	defer c.RUnlock()

	var stores []*storeRuntime
	for id := range cell.getStoreIDs() {
		if store := c.doGetStore(id); store != nil {
			stores = append(stores, store.clone())
		}
	}
	return stores
}

func (c *cache) foreachStore(fn func(*storeRuntime) (bool, error)) error {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.sc.stores {
		next, err := fn(s)
		if err != nil {
			return err
		}

		if !next {
			break
		}
	}

	return nil
}

func (c *cache) doGetStore(storeID uint64) *storeRuntime {
	store, ok := c.sc.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

func (c *cache) getStore(storeID uint64) *storeRuntime {
	c.RLock()
	defer c.RUnlock()

	return c.doGetStore(storeID)
}

func (c *cache) addStore(store meta.Store) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.sc.stores[store.ID]; ok {
		return
	}

	c.sc.stores[store.ID] = newStoreRuntime(store)
}

func (c *cache) setStore(store *storeRuntime) {
	c.Lock()
	defer c.Unlock()
	c.sc.stores[store.store.ID] = store
}

func (c *cache) addCell(cell meta.Cell) {
	c.Lock()
	defer c.Unlock()

	c.cc.addCell(newCellRuntime(cell))
}

func (c *cache) getCell(id uint64) *cellRuntime {
	c.RLock()
	defer c.RUnlock()

	return c.cc.cells[id]
}

func (c *cache) doCellHeartbeat(source meta.Cell) error {
	current := c.getCell(source.ID)

	// add new cell
	if nil == current {
		return c.doSave(source)
	}

	// update cell
	currentEpoch := current.cell.Epoch
	sourceEpoch := source.Epoch

	// cell meta is stale, return an error.
	if sourceEpoch.CellVer < currentEpoch.CellVer ||
		sourceEpoch.ConfVer < currentEpoch.ConfVer {
		log.Warnf("cell-heartbeat: cell is stale, cell=<%d> current<%d,%d> source<%d,%d>",
			source.ID,
			currentEpoch.CellVer,
			currentEpoch.ConfVer,
			sourceEpoch.CellVer,
			sourceEpoch.ConfVer)
		return errStaleCell
	}

	// cell meta is updated, update kv and cache.
	if sourceEpoch.CellVer > currentEpoch.CellVer ||
		sourceEpoch.ConfVer > currentEpoch.ConfVer {
		log.Infof("cell-heartbeat: cell version updated, cell=<%d> cellVer=<%d->%d> confVer=<%d->%d>",
			source.ID,
			currentEpoch.CellVer,
			sourceEpoch.CellVer,
			currentEpoch.ConfVer,
			sourceEpoch.ConfVer)
		return c.doSave(source)
	}

	// cell meta is the same, update cache only.
	c.addCell(source)
	return nil
}

func (c *cache) doSave(cell meta.Cell) error {
	err := c.store.SetCellMeta(c.clusterID, cell)
	if err != nil {
		return err
	}
	c.addCell(cell)
	return nil
}

func (cc *cellCache) addCell(origin *cellRuntime) {
	if cell, ok := cc.cells[origin.getID()]; ok {
		cc.removeCell(cell)
	}

	// Add to tree and regions.
	cc.tree.Update(origin.cell)
	cc.cells[origin.getID()] = origin

	if origin.leader.ID <= 0 {
		return
	}

	// Add to leaders and followers.
	for _, peer := range origin.getPeers() {
		storeID := peer.StoreID
		if peer.ID == origin.leader.ID {
			// Add leader peer to leaders.
			store, ok := cc.leaders[storeID]
			if !ok {
				store = make(map[uint64]*cellRuntime)
				cc.leaders[storeID] = store
			}
			store[origin.getID()] = origin
		} else {
			// Add follower peer to followers.
			store, ok := cc.followers[storeID]
			if !ok {
				store = make(map[uint64]*cellRuntime)
				cc.followers[storeID] = store
			}
			store[origin.getID()] = origin
		}
	}
}

func (cc *cellCache) removeCell(origin *cellRuntime) {
	// Remove from tree and cells.
	cc.tree.Remove(origin.cell)
	delete(cc.cells, origin.getID())

	// Remove from leaders and followers.
	for _, peer := range origin.getPeers() {
		delete(cc.leaders[peer.StoreID], origin.getID())
		delete(cc.followers[peer.StoreID], origin.getID())
	}
}
