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

	"github.com/deepfabric/elasticell/pkg/storage/meta"
)

const (
	batchLimit = 10000
)

type clusterRuntime struct {
	cluster *meta.ClusterMeta
}

func newClusterRuntime(cluster *meta.ClusterMeta) *clusterRuntime {
	return &clusterRuntime{
		cluster: cluster,
	}
}

type cache struct {
	sync.RWMutex
	cluster *clusterRuntime
	sc      *storeCache
	cc      *cellCache
}

func newCache() *cache {
	c := new(cache)
	c.sc = newStoreCache()
	c.cc = newCellCache()

	return c
}

func (c *cache) addStore(store *meta.StoreMeta) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.sc.stores[store.ID]; ok {
		return
	}

	c.sc.stores[store.ID] = newStoreRuntime(store)
}

func (c *cache) addCell(cell *meta.CellMeta) {
	c.Lock()
	defer c.Unlock()

	c.cc.addCell(newCellRuntime(cell))
}
