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

type nemoDataEngine struct {
	db *gonemo.NEMO
}

func newNemoDataEngine(db *gonemo.NEMO) DataEngine {
	return &nemoDataEngine{
		db: db,
	}
}

func (e *nemoDataEngine) RangeDelete(start, end []byte) error {
	e.db.RangeDel(start, end, 0)
	// TODO: rocksdb must return a error
	return nil
}

// Scan scans the range and execute the handler fun.
// returns false means end the scan.
func (e *nemoDataEngine) Scan(startKey []byte, endKey []byte, handler func(key, value []byte) (bool, error)) error {
	return errors.New("data engine not implement Scan")
}

func (e *nemoDataEngine) ScanSize(startKey []byte, endKey []byte, handler func(key []byte, size uint64) (bool, error)) error {
	var err error
	c := false

	it := e.db.NewVolumeIterator(startKey, endKey, 0)
	for ; it.Valid(); it.Next() {
		c, err = handler(it.Key(), uint64(it.Value()))
		if err != nil || !c {
			break
		}
	}
	it.Free()

	return err
}

func (e *nemoDataEngine) CompactRange(startKey []byte, endKey []byte) error {
	// TODO: impl
	return nil
}

// Seek the first key >= given key, if no found, return None.
func (e *nemoDataEngine) Seek(key []byte) ([]byte, []byte, error) {
	// TODO: impl
	return nil, nil, nil
}
