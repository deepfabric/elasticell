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

func (e *nemoDataEngine) GetTargetSizeKey(startKey []byte, endKey []byte, size uint64) (uint64, []byte, error) {
	var currentSize uint64
	var targetKey []byte

	it := e.db.NewVolumeIterator(startKey, endKey)
	if it.TargetScan(int64(size)) {
		targetKey = it.TargetKey()
	} else {
		currentSize = uint64(it.TotalVolume())
	}

	return currentSize, targetKey, nil
}

// CreateSnapshot create a snapshot file under the giving path
func (e *nemoDataEngine) CreateSnapshot(path string, start, end []byte) error {
	return e.db.RawScanSaveRange(path, start, end, true)
}

// ApplySnapshot apply a snapshort file from giving path
func (e *nemoDataEngine) ApplySnapshot(path string) error {
	return e.db.IngestFile(path)
}
