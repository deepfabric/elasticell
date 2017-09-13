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
	"github.com/deepfabric/elasticell/pkg/util"
)

type memoryMetaEngine struct {
	kv *util.KVTree
}

func newMemoryMetaEngine() Engine {
	return &memoryMetaEngine{
		kv: util.NewKVTree(),
	}
}

func (e *memoryMetaEngine) Set(key []byte, value []byte) error {
	e.kv.Put(key, value)
	return nil
}

func (e *memoryMetaEngine) Get(key []byte) ([]byte, error) {
	return e.kv.Get(key), nil
}

func (e *memoryMetaEngine) Delete(key []byte) error {
	e.kv.Delete(key)
	return nil
}

func (e *memoryMetaEngine) RangeDelete(start, end []byte) error {
	e.kv.RangeDelete(start, end)
	return nil
}

// Scan scans the range and execute the handler fun.
// returns false means end the scan.
func (e *memoryMetaEngine) Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	return e.kv.Scan(start, end, handler)
}

// Free free unsafe the key or value
func (e *memoryMetaEngine) Free(unsafe []byte) {

}

// Seek the first key >= given key, if no found, return None.
func (e *memoryMetaEngine) Seek(key []byte) ([]byte, []byte, error) {
	k, v := e.kv.Seek(key)
	return k, v, nil
}
