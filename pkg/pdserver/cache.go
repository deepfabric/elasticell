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
	"github.com/pkg/errors"
)

const (
	batchLimit = 10000
)

func newCache(clusterID uint64, store Store, allocator *idAllocator) *cache {
	c := new(cache)
	c.clusterID = clusterID
	c.sc = newStoreCache()
	c.cc = newCellCache()
	c.store = store
	c.allocator = allocator

	return c
}

func newClusterRuntime(cluster meta.Cluster) *clusterRuntime {
	return &clusterRuntime{
		cluster: cluster,
	}
}

type clusterRuntime struct {
	cluster meta.Cluster
}

type cache struct {
	sync.RWMutex
	clusterID uint64
	cluster   *clusterRuntime
	sc        *storeCache
	cc        *cellCache
	store     Store
	allocator *idAllocator
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

func (c *cache) getStores() []*storeRuntimeInfo {
	c.RLock()
	defer c.RUnlock()

	stores := make([]*storeRuntimeInfo, 0, len(c.sc.stores))
	for _, store := range c.sc.stores {
		stores = append(stores, store.clone())
	}
	return stores
}

func (c *cache) getCellStores(cell *cellRuntimeInfo) []*storeRuntimeInfo {
	c.RLock()
	defer c.RUnlock()

	var stores []*storeRuntimeInfo
	for id := range cell.getStoreIDs() {
		if store := c.doGetStore(id); store != nil {
			stores = append(stores, store.clone())
		}
	}
	return stores
}

func (c *cache) randFollowerCell(storeID uint64) *cellRuntimeInfo {
	c.RLock()
	defer c.RUnlock()

	return c.cc.randFollowerCell(storeID)
}

func (c *cache) randLeaderCell(storeID uint64) *cellRuntimeInfo {
	c.RLock()
	defer c.RUnlock()

	return c.cc.randLeaderCell(storeID)
}

func (c *cache) getFollowerStores(cell *cellRuntimeInfo) []*storeRuntimeInfo {
	c.RLock()
	defer c.RUnlock()

	var stores []*storeRuntimeInfo
	for id := range cell.getFollowers() {
		if store := c.sc.stores[id]; store != nil {
			stores = append(stores, store)
		}
	}
	return stores
}

func (c *cache) foreachStore(fn func(*storeRuntimeInfo) (bool, error)) error {
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

func (c *cache) searchCell(startKey []byte) *cellRuntimeInfo {
	cell := c.cc.tree.Search(startKey)
	if cell.ID == 0 {
		return nil
	}

	return c.getCell(cell.ID)
}

func (c *cache) getStore(storeID uint64) *storeRuntimeInfo {
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

func (c *cache) setStore(store *storeRuntimeInfo) {
	c.Lock()
	defer c.Unlock()
	c.sc.stores[store.store.ID] = store
}

func (c *cache) addCellFromMeta(cell meta.Cell) {
	c.Lock()
	defer c.Unlock()

	c.cc.addCell(newCellRuntimeInfo(cell, nil))
}

func (c *cache) addCell(source *cellRuntimeInfo) {
	c.Lock()
	defer c.Unlock()

	c.cc.addCell(source)
}

func (c *cache) getCell(id uint64) *cellRuntimeInfo {
	c.RLock()
	defer c.RUnlock()

	return c.cc.cells[id]
}

func (c *cache) doGetStore(storeID uint64) *storeRuntimeInfo {
	store, ok := c.sc.stores[storeID]
	if !ok {
		return nil
	}
	return store
}

func (c *cache) doCellHeartbeat(source *cellRuntimeInfo) error {
	current := c.getCell(source.cell.ID)

	// add new cell
	if nil == current {
		return c.doSave(source)
	}

	// update cell
	currentEpoch := current.cell.Epoch
	sourceEpoch := source.cell.Epoch

	// cell meta is stale, return an error.
	if sourceEpoch.CellVer < currentEpoch.CellVer ||
		sourceEpoch.ConfVer < currentEpoch.ConfVer {
		log.Warnf("cell-heartbeat[%d]: cell is stale, current<%d,%d> source<%d,%d>",
			source.cell.ID,
			currentEpoch.CellVer,
			currentEpoch.ConfVer,
			sourceEpoch.CellVer,
			sourceEpoch.ConfVer)
		return errStaleCell
	}

	// cell meta is updated, update kv and cache.
	if sourceEpoch.CellVer > currentEpoch.CellVer ||
		sourceEpoch.ConfVer > currentEpoch.ConfVer {
		log.Infof("cell-heartbeat[%d]: cell version updated, cellVer=<%d->%d> confVer=<%d->%d>",
			source.cell.ID,
			currentEpoch.CellVer,
			sourceEpoch.CellVer,
			currentEpoch.ConfVer,
			sourceEpoch.ConfVer)
		return c.doSave(source)
	}

	if current.leader.ID != source.leader.ID {
		log.Infof("cell-heartbeat[%d]: update cell leader, from=<%v> to=<%+v>",
			current.getID(),
			current,
			source)
	}

	// cell meta is the same, update cache only.
	c.addCell(source)
	return nil
}

func (c *cache) doSave(source *cellRuntimeInfo) error {
	err := c.store.SetCellMeta(c.clusterID, source.cell)
	if err != nil {
		return err
	}
	c.addCell(source)
	return nil
}

func (cc *cellCache) addCell(origin *cellRuntimeInfo) {
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
				store = make(map[uint64]*cellRuntimeInfo)
				cc.leaders[storeID] = store
			}
			store[origin.getID()] = origin
		} else {
			// Add follower peer to followers.
			store, ok := cc.followers[storeID]
			if !ok {
				store = make(map[uint64]*cellRuntimeInfo)
				cc.followers[storeID] = store
			}
			store[origin.getID()] = origin
		}
	}
}

func (cc *cellCache) removeCell(origin *cellRuntimeInfo) {
	// Remove from tree and cells.
	cc.tree.Remove(origin.cell)
	delete(cc.cells, origin.getID())

	// Remove from leaders and followers.
	for _, peer := range origin.getPeers() {
		delete(cc.leaders[peer.StoreID], origin.getID())
		delete(cc.followers[peer.StoreID], origin.getID())
	}
}

func randCell(cells map[uint64]*cellRuntimeInfo) *cellRuntimeInfo {
	for _, cell := range cells {
		if cell.leader == nil {
			log.Fatalf("rand cell without leader: cell=<%+v>", cell)
		}

		if len(cell.downPeers) > 0 {
			continue
		}

		if len(cell.pendingPeers) > 0 {
			continue
		}

		return cell.clone()
	}

	return nil
}
