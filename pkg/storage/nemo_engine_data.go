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

type nemoDataEngine struct {
	db *gonemo.NEMO
}

func newNemoDataEngine(db *gonemo.NEMO) DataEngine {
	return &nemoDataEngine{
		db: db,
	}
}

func (e *nemoDataEngine) RangeDelete(start, end []byte) error {
	return e.db.RangeDel(start, end)
}

func (e *nemoDataEngine) ScanSize(startKey []byte, endKey []byte, handler func(key []byte, size uint64) (bool, error)) error {
	var err error
	c := false

	it := e.db.NewVolumeIterator(startKey, endKey)
	for ; it.Valid(); it.Next() {
		c, err = handler(it.Key(), uint64(it.Value()))
		if err != nil || !c {
			break
		}
	}
	it.Free()

	return err
}

// CreateSnapshot create a snapshot file under the giving path
func (e *nemoDataEngine) CreateSnapshot(path string, start, end []byte) error {
	return e.db.RawScanSaveRange(path, start, end, true)
}

// ApplySnapshot apply a snapshort file from giving path
func (e *nemoDataEngine) ApplySnapshot(path string) error {
	return e.db.IngestFile(path)
}
