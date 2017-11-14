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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/pkg/errors"
)

const (
	batchLimit = 10000
)

func newCache(clusterID uint64, store Store, allocator *idAllocator, notify *watcherNotifier) *cache {
	c := new(cache)
	c.clusterID = clusterID
	c.sc = newStoreCache()
	c.cc = newCellCache()
	c.store = store
	c.allocator = allocator
	c.notify = notify

	return c
}

func newClusterRuntime(cluster metapb.Cluster) *ClusterInfo {
	return &ClusterInfo{
		Meta: cluster,
	}
}

// ClusterInfo The cluster info
type ClusterInfo struct {
	Meta metapb.Cluster `json:"meta"`
}

type cache struct {
	sync.RWMutex

	clusterID uint64
	cluster   *ClusterInfo
	sc        *storeCache
	cc        *cellCache

	allocator *idAllocator
	notify    *watcherNotifier

	store Store
}

func (c *cache) getStoreCache() *storeCache {
	return c.sc
}

func (c *cache) getCellCache() *cellCache {
	return c.cc
}

func (c *cache) allocPeer(storeID uint64, allocPeerID bool) (metapb.Peer, error) {
	if allocPeerID {
		peerID, err := c.allocator.newID()
		if err != nil {
			return metapb.Peer{}, errors.Wrap(err, "")
		}

		return metapb.Peer{
			ID:      peerID,
			StoreID: storeID,
		}, nil
	}

	return metapb.Peer{
		StoreID: storeID,
	}, nil
}

func (c *cache) handleCellHeartbeat(source *CellInfo) error {
	current := c.getCellCache().getCell(source.Meta.ID)

	// add new cell
	if nil == current {
		err := c.doSaveCellInfo(source)
		if err != nil {
			return err
		}

		c.notifyChangedRange(source.Meta.ID)
		return nil
	}

	// update cell
	currentEpoch := current.Meta.Epoch
	sourceEpoch := source.Meta.Epoch

	// cell meta is stale, return an error.
	if sourceEpoch.CellVer < currentEpoch.CellVer ||
		sourceEpoch.ConfVer < currentEpoch.ConfVer {
		log.Warnf("cell-heartbeat[%d]: cell is stale, current<%d,%d> source<%d,%d>",
			source.Meta.ID,
			currentEpoch.CellVer,
			currentEpoch.ConfVer,
			sourceEpoch.CellVer,
			sourceEpoch.ConfVer)
		return errStaleCell
	}

	rangeChanged := sourceEpoch.CellVer > currentEpoch.CellVer
	peersChanged := sourceEpoch.ConfVer > currentEpoch.ConfVer
	// cell meta is updated, update kv and cache.
	if rangeChanged || peersChanged {
		log.Infof("cell-heartbeat[%d]: cell version updated, cellVer=<%d->%d> confVer=<%d->%d>",
			source.Meta.ID,
			currentEpoch.CellVer,
			sourceEpoch.CellVer,
			currentEpoch.ConfVer,
			sourceEpoch.ConfVer)
		err := c.doSaveCellInfo(source)
		if err != nil {
			return err
		}

		if rangeChanged {
			c.notifyChangedRange(source.Meta.ID)
		}

		return nil
	}

	leaderChanged := false
	if current.LeaderPeer != nil && current.LeaderPeer.ID != source.LeaderPeer.ID {
		log.Infof("cell-heartbeat[%d]: update cell leader, from=<%v> to=<%+v>",
			current.getID(),
			current,
			source)
		leaderChanged = true
	}

	// cell meta is the same, update cache only.
	c.getCellCache().addOrUpdate(source)

	if leaderChanged {
		c.notifyChangedRange(source.Meta.ID)
	}

	return nil
}

func (c *cache) doSaveCellInfo(source *CellInfo) error {
	err := c.store.SetCellMeta(c.clusterID, source.Meta)
	if err != nil {
		return err
	}

	c.getCellCache().addOrUpdate(source)
	return nil
}

func (c *cache) notifyChangedRange(id uint64) {
	cr := c.getCellCache().getCell(id)
	r := &pdpb.Range{
		Cell: cr.Meta,
	}
	if cr.LeaderPeer != nil {
		r.LeaderStore = c.getStoreCache().getStore(cr.LeaderPeer.StoreID).Meta
	}

	c.notify.notify(r)
}

func randCell(cells map[uint64]*CellInfo) *CellInfo {
	for _, cell := range cells {
		if cell.LeaderPeer == nil {
			log.Fatalf("rand cell without leader: cell=<%+v>", cell)
		}

		if len(cell.DownPeers) > 0 {
			continue
		}

		if len(cell.PendingPeers) > 0 {
			continue
		}

		return cell.clone()
	}

	return nil
}
