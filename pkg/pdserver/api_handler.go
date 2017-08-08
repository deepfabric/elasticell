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
	"fmt"
	"sort"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pdapi"
)

// ListCell returns all cells info
func (s *Server) ListCell() ([]*pdapi.CellInfo, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	return toAPICellSlice(cluster.cache.getCellCache().getCells()), nil
}

// GetCell return the cell with the id
func (s *Server) GetCell(id uint64) (*pdapi.CellInfo, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	c := cluster.cache.getCellCache()
	return toAPICell(c.getCell(id)), nil
}

// ListStore returns all store info
func (s *Server) ListStore() ([]*pdapi.StoreInfo, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	return toAPIStoreSlice(cluster.cache.getStoreCache().getStores()), nil
}

// GetStore return the store with the id
func (s *Server) GetStore(id uint64) (*pdapi.StoreInfo, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	c := cluster.cache.getStoreCache()
	return toAPIStore(c.getStore(id)), nil
}

// DeleteStore remove the store from cluster
// all cells on this store will move to another stores
func (s *Server) DeleteStore(id uint64, force bool) error {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return errNotBootstrapped
	}

	var store *StoreInfo
	var err error
	if force {
		store, err = cluster.cache.getStoreCache().setStoreTombstone(id, force)
	} else {
		store, err = cluster.cache.getStoreCache().setStoreOffline(id)
	}

	if err != nil {
		return err
	}

	if nil != store {
		err = s.store.SetStoreMeta(id, store.Meta)
		if err != nil {
			return err
		}

		log.Infof("[store-%d] store has been %s, address=<%s>",
			store.Meta.ID,
			store.Meta.State.String(),
			store.Meta.Address)
		cluster.cache.getStoreCache().updateStoreInfo(store)
	}

	return nil
}

// TransferLeader transfer cell leader to the spec peer
func (s *Server) TransferLeader(transfer *pdapi.TransferLeader) error {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return errNotBootstrapped
	}

	cellInfo := cluster.cache.getCellCache().getCell(transfer.CellID)
	if nil == cellInfo {
		return fmt.Errorf("Cell %d not found", transfer.CellID)
	}

	peer := cellInfo.getPeer(transfer.ToPeerID)
	if nil == peer {
		return fmt.Errorf("Peer %d not found", transfer.ToPeerID)
	}

	sc := cluster.coordinator.getScheduler(balanceLeaderSchedulerName)
	if sc == nil {
		return fmt.Errorf("Scheduler not found")
	}

	sc.Lock()
	defer sc.Unlock()
	if !sc.AllowSchedule() {
		return fmt.Errorf("Scheduler not performed by limit")
	}

	cluster.coordinator.addOperator(newTransferLeaderAggregationOp(cellInfo, peer))
	return nil
}

// GetOperator get current operator with id
func (s *Server) GetOperator(id uint64) (interface{}, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	cellInfo := cluster.cache.getCellCache().getCell(id)
	if nil == cellInfo {
		return nil, fmt.Errorf("Cell %d not found", id)
	}

	return cluster.coordinator.getOperator(id), nil
}

func toAPIStore(store *StoreInfo) *pdapi.StoreInfo {
	return &pdapi.StoreInfo{
		Meta: store.Meta,
		Status: &pdapi.StoreStatus{
			Stats:           store.Status.Stats,
			LeaderCount:     store.Status.LeaderCount,
			LastHeartbeatTS: store.Status.LastHeartbeatTS.Unix(),
		},
	}
}

func toAPICell(cell *CellInfo) *pdapi.CellInfo {
	return &pdapi.CellInfo{
		Meta:         cell.Meta,
		LeaderPeer:   cell.LeaderPeer,
		DownPeers:    cell.DownPeers,
		PendingPeers: cell.PendingPeers,
	}
}

func toAPIStoreSlice(stores []*StoreInfo) []*pdapi.StoreInfo {
	values := make([]*pdapi.StoreInfo, len(stores))

	for idx, store := range stores {
		values[idx] = toAPIStore(store)
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Meta.ID < values[j].Meta.ID
	})
	return values
}

func toAPICellSlice(cells []*CellInfo) []*pdapi.CellInfo {
	values := make([]*pdapi.CellInfo, len(cells))

	for idx, cell := range cells {
		values[idx] = toAPICell(cell)
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i].Meta.ID < values[j].Meta.ID
	})
	return values
}
