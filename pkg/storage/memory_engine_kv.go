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

type memoryKVEngine struct {
	kv *util.KVTree
}

func newMemoryKVEngine(kv *util.KVTree) KVEngine {
	return &memoryKVEngine{
		kv: kv,
	}
}

func (e *memoryKVEngine) Set(key, value []byte) error {
	e.kv.Put(key, value)
	return nil
}

func (e *memoryKVEngine) MSet(keys [][]byte, values [][]byte) error {
	for i := 0; i < len(keys); i++ {
		e.kv.Put(keys[i], values[i])
	}

	return nil
}

func (e *memoryKVEngine) Get(key []byte) ([]byte, error) {
	v := e.kv.Get(key)
	return v, nil
}

func (e *memoryKVEngine) IncrBy(key []byte, incrment int64) (int64, error) {
	var value int64

	v := e.kv.Get(key)
	if len(v) != 0 {
		value, _ = util.StrInt64(v)
	}

	value = value + incrment
	e.kv.Put(key, util.FormatInt64ToBytes(value))
	return value, nil
}

func (e *memoryKVEngine) DecrBy(key []byte, incrment int64) (int64, error) {
	return 0, nil
}

func (e *memoryKVEngine) GetSet(key, value []byte) ([]byte, error) {
	return nil, nil
}

func (e *memoryKVEngine) Append(key, value []byte) (int64, error) {
	return 0, nil
}

func (e *memoryKVEngine) SetNX(key, value []byte) (int64, error) {
	return 0, nil
}

func (e *memoryKVEngine) StrLen(key []byte) (int64, error) {
	return 0, nil
}

func (e *memoryKVEngine) NewWriteBatch() WriteBatch {
	return newMemoryWriteBatch()
}

func (e *memoryKVEngine) Write(wb WriteBatch) error {
	mwb := wb.(*memoryWriteBatch)

	for _, opt := range mwb.opts {
		if opt.isDelete {
			e.kv.Delete(opt.key)
		} else {
			e.kv.Put(opt.key, opt.value)
		}
	}

	return nil
}
