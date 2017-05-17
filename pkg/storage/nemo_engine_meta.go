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
	"errors"

	gonemo "github.com/deepfabric/go-nemo"
)

type nemoMetaEngine struct {
	db      *gonemo.NEMO
	handler *gonemo.DBWithTTL
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
	e.db.GetWithHandle(e.handler, key)
	// TODO: rocksdb must return a error
	return nil
}

func (e *nemoMetaEngine) RangeDelete(start, end []byte) error {
	e.db.RangeDelWithHandle(e.handler, start, end, 0)
	// TODO: rocksdb must return a error
	return nil
}

// Scan scans the range and execute the handler fun.
// returns false means end the scan.
func (e *nemoMetaEngine) Scan(startKey []byte, endKey []byte, handler func(key, value []byte) (bool, error)) error {
	var err error
	c := false

	it := e.db.KScanWithHandle(e.handler, startKey, endKey, 0)
	for ; it.Valid(); it.Next() {
		c, err = handler(it.Key(), it.Value())
		if err != nil || !c {
			break
		}
	}
	it.Free()

	return err
}

func (e *nemoMetaEngine) ScanSize(startKey []byte, endKey []byte, handler func(key []byte, size uint64) (bool, error)) error {
	return errors.New("meta engine not implement ScanSize")
}

func (e *nemoMetaEngine) CompactRange(startKey []byte, endKey []byte) error {
	// TODO: impl
	return nil
}

// Seek the first key >= given key, if no found, return None.
func (e *nemoMetaEngine) Seek(key []byte) ([]byte, []byte, error) {
	// TODO: impl
	return nil, nil, nil
}
