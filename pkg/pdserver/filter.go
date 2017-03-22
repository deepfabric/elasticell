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

const (
	storageRatioThreshold = 80
)

// Filter is used for filter store
type Filter interface {
	FilterSource(store *storeRuntime) bool
	FilterTarget(store *storeRuntime) bool
}

type stateFilter struct {
	cfg *Cfg
}

// storageThresholdFilter ensures that we will not use an almost full store as a target.
type storageThresholdFilter struct {
	cfg *Cfg
}

type excludedFilter struct {
	sources map[uint64]struct{}
	targets map[uint64]struct{}
}

func newStorageThresholdFilter(cfg *Cfg) *storageThresholdFilter {
	return &storageThresholdFilter{
		cfg: cfg,
	}
}

func newStateFilter(cfg *Cfg) Filter {
	return &stateFilter{cfg: cfg}
}

func newExcludedFilter(sources, targets map[uint64]struct{}) *excludedFilter {
	return &excludedFilter{
		sources: sources,
		targets: targets,
	}
}

func filterTarget(store *storeRuntime, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(store) {
			return true
		}
	}
	return false
}

func (f *stateFilter) filter(store *storeRuntime) bool {
	return !store.isUp()
}

func (f *stateFilter) FilterSource(store *storeRuntime) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *storeRuntime) bool {
	return f.filter(store)
}

func (f *storageThresholdFilter) FilterSource(store *storeRuntime) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *storeRuntime) bool {
	return store.storageRatio() > storageRatioThreshold
}

func (f *excludedFilter) FilterSource(store *storeRuntime) bool {
	_, ok := f.sources[store.getID()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *storeRuntime) bool {
	_, ok := f.targets[store.getID()]
	return ok
}
