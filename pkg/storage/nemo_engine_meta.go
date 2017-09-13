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

// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	gonemo "github.com/deepfabric/go-nemo"
)

type nemoMetaEngine struct {
	db      *gonemo.NEMO
	handler *gonemo.DBNemo
}

func newNemoMetaEngine(db *gonemo.NEMO) Engine {
	return &nemoMetaEngine{
		db:      db,
		handler: db.GetMetaHandle(),
	}
}

func (e *nemoMetaEngine) Set(key []byte, value []byte) error {
	return e.db.PutWithHandle(e.handler, key, value)
}

func (e *nemoMetaEngine) Get(key []byte) ([]byte, error) {
	return e.db.GetWithHandle(e.handler, key)
}

func (e *nemoMetaEngine) Delete(key []byte) error {
	return e.db.DeleteWithHandle(e.handler, key)
}

func (e *nemoMetaEngine) RangeDelete(start, end []byte) error {
	return e.db.RangeDelWithHandle(e.handler, start, end)
}

// Scan scans the range and execute the handler fun.
// returns false means end the scan.
func (e *nemoMetaEngine) Scan(startKey []byte, endKey []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	var key []byte
	var err error
	c := false

	it := e.db.KScanWithHandle(e.handler, startKey, endKey, true)
	for ; it.Valid(); it.Next() {
		if pooledKey {
			key = it.PooledKey()
		} else {
			key = it.Key()
		}

		c, err = handler(key, it.Value())
		if err != nil || !c {
			break
		}
	}
	it.Free()

	return err
}

// Free free unsafe the key or value
func (e *nemoMetaEngine) Free(unsafe []byte) {
	gonemo.MemPool.Free(unsafe)
}

// Seek the first key >= given key, if no found, return None.
func (e *nemoMetaEngine) Seek(key []byte) ([]byte, []byte, error) {
	return e.db.SeekWithHandle(e.handler, key)
}
