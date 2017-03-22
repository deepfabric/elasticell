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
	"github.com/deepfabric/elasticell/pkg/meta"
)

type storeRuntime struct {
	store *meta.StoreMeta
}

func newStoreRuntime(store *meta.StoreMeta) *storeRuntime {
	return &storeRuntime{
		store: store,
	}
}

func (s *storeRuntime) getID() uint64 {
	return s.store.ID
}

func (s *storeRuntime) isUp() bool {
	return s.store.State == meta.StoreStateUp
}

func (s *storeRuntime) storageRatio() int {
	if s.store.Metrics.Capacity == 0 {
		return 0
	}

	return int(float64(s.storageSize()) * 100 / float64(s.store.Metrics.Capacity))
}

func (s *storeRuntime) storageSize() uint64 {
	return s.store.Metrics.Capacity - s.store.Metrics.Available
}

func (s *storeRuntime) cellScore() float64 {
	if s.store.Metrics.Capacity == 0 {
		return 0
	}

	return float64(s.store.Metrics.CellCount) / float64(s.store.Metrics.Capacity)
}

func (s *storeRuntime) getLocationID(keys []string) string {
	id := ""
	for _, k := range keys {
		v := s.getLabelValue(k)
		if len(v) == 0 {
			return ""
		}
		id += v
	}
	return id
}

func (s *storeRuntime) getLabelValue(key string) string {
	for _, label := range s.store.Lables {
		if label.Key == key {
			return label.Value
		}
	}

	return ""
}
