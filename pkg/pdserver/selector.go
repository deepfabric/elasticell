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

// Selector is an interface to select source and target store to schedule.
type Selector interface {
	SelectSource(stores []*StoreInfo, filters ...Filter) *StoreInfo
	SelectTarget(stores []*StoreInfo, filters ...Filter) *StoreInfo
}

func newBalanceSelector(kind ResourceKind, filters []Filter) *balanceSelector {
	return &balanceSelector{
		kind:    kind,
		filters: filters,
	}
}

type balanceSelector struct {
	kind    ResourceKind
	filters []Filter
}

func (s *balanceSelector) SelectSource(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	filters = append(filters, s.filters...)

	var result *StoreInfo
	for _, store := range stores {
		if filterSource(store, filters) {
			continue
		}
		if result == nil || result.resourceScore(s.kind) < store.resourceScore(s.kind) {
			result = store
		}
	}
	return result
}

func (s *balanceSelector) SelectTarget(stores []*StoreInfo, filters ...Filter) *StoreInfo {
	filters = append(filters, s.filters...)

	var result *StoreInfo
	for _, store := range stores {
		if filterTarget(store, filters) {
			continue
		}
		if result == nil || result.resourceScore(s.kind) > store.resourceScore(s.kind) {
			result = store
		}
	}
	return result
}
