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

package node

import (
	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
)

func (n *Node) deleteCell(id uint64) error {
	err := n.metaDB.DeleteCell(id)
	if err != nil {
		return err
	}

	n.deleteCellInMem(id)
	log.Infof("meta: delete cell succ, cell=<%d>", id)
	return nil
}

func (n *Node) newStore(id uint64) meta.Store {
	return meta.Store{
		ID:      id,
		Address: n.cfg.StoreAddr,
		Lables:  n.cfg.StoreLables,
		State:   meta.UP,
		Metric:  n.getCurrentStoreMetrics(),
	}
}

func (n *Node) getCurrentStoreMetrics() *meta.StoreMetric {
	return &meta.StoreMetric{
		Capacity:  n.metaDB.GetTotalDiskSize(),
		Available: n.metaDB.GetAvailableDiskSize(),
		CellCount: uint64(len(n.cells)),
	}
}

func (n *Node) deleteCellInMem(id uint64) {
	n.Lock()
	delete(n.cells, id)
	n.Unlock()
}
