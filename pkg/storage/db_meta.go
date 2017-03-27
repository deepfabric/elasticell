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

package storage

import (
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/util"
)

// MetaDB is used for data persistent.
type MetaDB struct {
	cfg    *Cfg
	driver Driver
}

// NewMetaDB returns initial store
func NewMetaDB(cfg *Cfg, driver Driver) *MetaDB {
	// TODO: create driver
	return &MetaDB{
		cfg:    cfg,
		driver: driver,
	}
}

// GetTotalDiskSize returns the total disk space
func (db *MetaDB) GetTotalDiskSize() uint64 {
	return util.TotalDisk(db.cfg.DiskPartitionPath)
}

// GetAvailableDiskSize returns the available disk space
func (db *MetaDB) GetAvailableDiskSize() uint64 {
	// TODO: GetTotalDiskSize() - used
	return 0
}

// // CreateStoreInfo use store and cluster id init
// func (s *Store) CreateStoreInfo(storeID uint64, addr string, labels []*meta.Label) {
// 	s.mux.Lock()
// 	s.meta.ID = storeID
// 	s.meta.Address = addr
// 	s.meta.Lables = labels
// 	s.meta.State = meta.StoreStateUp
// 	s.meta.Metrics = &meta.StoreMetrics{
// 		Capacity:  s.GetTotalDiskSize(),
// 		Available: s.GetAvailableDiskSize(),
// 		CellCount: s.GetCellCount(),
// 	}
// 	s.mux.Unlock()
// }

// // GetStoreMeta returns meta data of store
// func (s *Store) GetStoreMeta() ([]byte, error) {
// 	s.mux.RLock()
// 	data, err := s.meta.Marshal()
// 	s.mux.RUnlock()

// 	return data, err
// }

// SaveStore returns error when save the store to driver
func (db *MetaDB) SaveStore(meta meta.Store) error {
	return nil
}

// SaveCell returns error when save the cell to driver
func (db *MetaDB) SaveCell(meta meta.Cell) error {
	return nil
}

// DeleteCell delete local cell
func (db *MetaDB) DeleteCell(id uint64) error {
	return nil
}
