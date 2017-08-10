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

	"github.com/pkg/errors"
)

const (
	batch = uint64(1000)
)

type idAllocator struct {
	sync.Mutex
	store             Store
	leaderSignatureFn func() string
	base              uint64
	end               uint64
}

func newIDAllocator(store Store, leaderSignatureFn func() string) *idAllocator {
	return &idAllocator{
		store:             store,
		leaderSignatureFn: leaderSignatureFn,
	}
}

func (alloc *idAllocator) newID() (uint64, error) {
	alloc.Lock()
	defer alloc.Unlock()

	if alloc.base == alloc.end {
		end, err := alloc.generate()
		if err != nil {
			return 0, errors.Wrap(err, "")
		}

		alloc.end = end
		alloc.base = alloc.end - batch
	}

	alloc.base++
	return alloc.base, nil
}

func (alloc *idAllocator) generate() (uint64, error) {
	value, err := alloc.store.GetID()
	if err != nil {
		return 0, errors.Wrap(err, "")
	}

	max := value + batch

	// create id
	if value == 0 {
		max := value + batch
		err := alloc.store.CreateID(alloc.leaderSignatureFn(), max)
		if err != nil {
			return 0, err
		}

		return max, nil
	}

	err = alloc.store.UpdateID(alloc.leaderSignatureFn(), value, max)
	if err != nil {
		return 0, err
	}

	return max, nil
}
