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
	"encoding/json"
	"fmt"
	"sort"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pdapi"
)

func (s *Server) getInitParams() (*pdapi.InitParams, error) {
	value, err := s.GetInitParamsValue()
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, nil
	}

	params := &pdapi.InitParams{}
	err = json.Unmarshal(value, params)
	return params, err
}

// GetSystem returns the summary of elasticell cluster
func (s *Server) GetSystem() (*pdapi.System, error) {
	system := &pdapi.System{
		AlreadyBootstrapped: false,
	}

	params, err := s.getInitParams()
	if err != nil {
		return nil, err
	}

	cluster := s.GetCellCluster()
	if nil == cluster {
		system.InitParams = params
		return system, nil
	}

	system.AlreadyBootstrapped = true
	system.MaxReplicas = s.cfg.Schedule.MaxReplicas
	system.OperatorCount = cluster.coordinator.getOperatorCount()
	system.InitParams = params

	cluster.cache.getStoreCache().foreach(func(store *StoreInfo) (bool, error) {
		system.StoreCount++

		if store.isOffline() {
			system.OfflineStoreCount++
		} else if store.isTombstone() {
			system.TombStoneStoreCount++
		} else {
			system.StorageCapacity += store.Status.Stats.Capacity
			system.StorageAvailable += store.Status.Stats.Available
		}

		return true, nil
	})

	cluster.cache.getCellCache().foreach(func(cell *CellInfo) (bool, error) {
		system.CellCount++

		if len(cell.Meta.Peers) < int(system.MaxReplicas) {
			system.ReplicasNotFullCellCount++
		}

		return true, nil
	})

	return system, nil
}

// InitCluster init cluster
func (s *Server) InitCluster(params *pdapi.InitParams) error {
	cluster := s.GetCellCluster()
	if nil != cluster {
		return errAlreadyBootstrapped
	}

	paramsStr, err := params.Marshal()
	if err != nil {
		return err
	}

	return s.store.SetInitParams(s.clusterID, paramsStr)
}

// ListCellInStore returns all cells info in the store
func (s *Server) ListCellInStore(storeID uint64) ([]*pdapi.CellInfo, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	var cells []*CellInfo
	cluster.cache.getCellCache().foreach(func(cell *CellInfo) (bool, error) {
		for _, p := range cell.Meta.Peers {
			if p.StoreID == storeID {
				cells = append(cells, cell.clone())
			}
		}

		return true, nil
	})

	return toAPICellSlice(cells), nil
}

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
		err = s.store.SetStoreMeta(s.GetClusterID(), store.Meta)
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

// SetStoreLogLevel set store log level
func (s *Server) SetStoreLogLevel(set *pdapi.SetLogLevel) error {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return errNotBootstrapped
	}

	for _, id := range set.Targets {
		cluster.coordinator.addStoreOperator(newSetLogLevelOperator(id, set.Level))
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

// GetOperators returns the current schedule operators
func (s *Server) GetOperators() ([]interface{}, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	return cluster.coordinator.getOperators(), nil
}

func (s *Server) ListIndex() (idxDefs []*pdpb.IndexDef, err error) {
	return s.store.ListIndex()
}

// GetIndex returns the info of given index
func (s *Server) GetIndex(id string) (idxDef *pdpb.IndexDef, err error) {
	return s.store.GetIndex(id)
}

func (s *Server) CreateIndex(idxDef *pdpb.IndexDef) (err error) {
	return s.store.CreateIndex(idxDef)
}
func (s *Server) DeleteIndex(id string) (err error) {
	return s.store.DeleteIndex(id)
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
