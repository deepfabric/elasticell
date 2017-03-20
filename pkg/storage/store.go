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
	"sync"

	"github.com/deepfabric/elasticell/pkg/storage/meta"
)

// Store is used for data persistent. It's contains many cells.
type Store struct {
	mux sync.RWMutex

	cfg    *Cfg
	driver Driver
	meta   *meta.StoreMeta
}

// NewStore returns initial store
func NewStore(cfg *Cfg) *Store {
	// TODO: create driver
	return &Store{
		cfg:  cfg,
		meta: meta.NewStoreMeta(),
	}
}

// InitStoreInfo use store and cluster id init
func (s *Store) InitStoreInfo(storeID uint64, addr string, labels []*meta.Label) {
	s.mux.Lock()
	s.meta.ID = storeID
	s.meta.Address = addr
	s.meta.Lables = labels
	s.mux.Unlock()
}

// GetStoreMeta returns meta data of store
func (s *Store) GetStoreMeta() ([]byte, error) {
	s.mux.RLock()
	data, err := s.meta.Marshal()
	s.mux.RUnlock()

	// TODO: get by driver

	return data, err
}

// Save returns error when save the store to driver
func (s *Store) Save() error {
	return nil
}

// GetStoreID returns store id
func (s *Store) GetStoreID() uint64 {
	if nil == s.meta {
		return 0
	}
	return s.meta.ID
}

// DeleteLocalCell delete local cell
func (s *Store) DeleteLocalCell(id uint64) error {
	return nil
}

// Start start the store
func (s *Store) Start() error {
	return nil
}

// Stop stop the store
func (s *Store) Stop() error {
	return nil
}
