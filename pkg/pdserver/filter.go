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

// Filter is used for filter store
type Filter interface {
	FilterSource(store *StoreInfo) bool
	FilterTarget(store *StoreInfo) bool
}

func filterSource(store *StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterSource(store) {
			return true
		}
	}
	return false
}

func filterTarget(store *StoreInfo, filters []Filter) bool {
	for _, filter := range filters {
		if filter.FilterTarget(store) {
			return true
		}
	}
	return false
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

func (f *stateFilter) filter(store *StoreInfo) bool {
	return !(store.isUp() && store.downTime() < f.cfg.Schedule.getMaxStoreDownTimeDuration())
}

func (f *stateFilter) FilterSource(store *StoreInfo) bool {
	return f.filter(store)
}

func (f *stateFilter) FilterTarget(store *StoreInfo) bool {
	return f.filter(store)
}

func (f *storageThresholdFilter) FilterSource(store *StoreInfo) bool {
	return false
}

func (f *storageThresholdFilter) FilterTarget(store *StoreInfo) bool {
	return store.storageRatio() > f.cfg.Schedule.StorageRatioThreshold
}

func (f *excludedFilter) FilterSource(store *StoreInfo) bool {
	_, ok := f.sources[store.getID()]
	return ok
}

func (f *excludedFilter) FilterTarget(store *StoreInfo) bool {
	_, ok := f.targets[store.getID()]
	return ok
}

type blockFilter struct{}

func newBlockFilter() *blockFilter {
	return &blockFilter{}
}

func (f *blockFilter) FilterSource(store *StoreInfo) bool {
	return store.isBlocked()
}

func (f *blockFilter) FilterTarget(store *StoreInfo) bool {
	return store.isBlocked()
}

type healthFilter struct {
	cfg *Cfg
}

func newHealthFilter(cfg *Cfg) *healthFilter {
	return &healthFilter{cfg: cfg}
}

func (f *healthFilter) filter(store *StoreInfo) bool {
	if store.Status == nil || store.Status.Stats.IsBusy {
		return true
	}

	return store.downTime() > f.cfg.Schedule.getMaxStoreDownTimeDuration()
}

func (f *healthFilter) FilterSource(store *StoreInfo) bool {
	return f.filter(store)
}

func (f *healthFilter) FilterTarget(store *StoreInfo) bool {
	return f.filter(store)
}

type cacheFilter struct {
	cache *idCache
}

func newCacheFilter(cache *idCache) *cacheFilter {
	return &cacheFilter{cache: cache}
}

func (f *cacheFilter) FilterSource(store *StoreInfo) bool {
	return f.cache.get(store.getID())
}

func (f *cacheFilter) FilterTarget(store *StoreInfo) bool {
	return false
}

type snapshotCountFilter struct {
	cfg *Cfg
}

func newSnapshotCountFilter(cfg *Cfg) *snapshotCountFilter {
	return &snapshotCountFilter{cfg: cfg}
}

func (f *snapshotCountFilter) filter(store *StoreInfo) bool {
	return uint64(store.Status.Stats.SendingSnapCount) > f.cfg.Schedule.MaxSnapshotCount ||
		uint64(store.Status.Stats.ReceivingSnapCount) > f.cfg.Schedule.MaxSnapshotCount ||
		uint64(store.Status.Stats.ApplyingSnapCount) > f.cfg.Schedule.MaxSnapshotCount
}

func (f *snapshotCountFilter) FilterSource(store *StoreInfo) bool {
	return f.filter(store)
}

func (f *snapshotCountFilter) FilterTarget(store *StoreInfo) bool {
	return f.filter(store)
}

// distinctScoreFilter ensures that distinct score will not decrease.
type distinctScoreFilter struct {
	cfg       *Cfg
	stores    []*StoreInfo
	safeScore float64
}

func newDistinctScoreFilter(cfg *Cfg, stores []*StoreInfo, source *StoreInfo) *distinctScoreFilter {
	newStores := make([]*StoreInfo, 0, len(stores)-1)
	for _, s := range stores {
		if s.getID() == source.getID() {
			continue
		}
		newStores = append(newStores, s)
	}

	return &distinctScoreFilter{
		cfg:       cfg,
		stores:    newStores,
		safeScore: cfg.getDistinctScore(newStores, source),
	}
}

func (f *distinctScoreFilter) FilterSource(store *StoreInfo) bool {
	return false
}

func (f *distinctScoreFilter) FilterTarget(store *StoreInfo) bool {
	return f.cfg.getDistinctScore(f.stores, store) < f.safeScore
}
