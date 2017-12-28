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
	"github.com/deepfabric/elasticell/pkg/util"
	gonemo "github.com/deepfabric/go-nemo"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type nemoDataEngine struct {
	db                *gonemo.NEMO
	limiter           *util.Limiter
	limiterTargetScan *util.Limiter
}

func newNemoDataEngine(db *gonemo.NEMO, cfg *NemoCfg) DataEngine {
	return &nemoDataEngine{
		limiter:           util.NewLimiter(cfg.LimitConcurrencyWrite),
		limiterTargetScan: util.NewLimiter(1),
		db:                db,
	}
}

func (e *nemoDataEngine) RangeDelete(start, end []byte) error {
	e.limiter.Wait(context.TODO())
	err := e.db.RangeDel(start, end)
	e.limiter.Release()

	return err
}

func (e *nemoDataEngine) GetTargetSizeKey(startKey []byte, endKey []byte, size uint64) (uint64, []byte, error) {
	var currentSize uint64
	var targetKey []byte

	e.limiterTargetScan.Wait(context.TODO())
	it := e.db.NewVolumeIterator(startKey, endKey)
	if it.TargetScan(int64(size)) {
		targetKey = it.TargetKey()
	} else {
		currentSize = uint64(it.TotalVolume())
	}
	it.Free()
	e.limiterTargetScan.Release()
	return currentSize, targetKey, nil
}

// CreateSnapshot create a snapshot file under the giving path
func (e *nemoDataEngine) CreateSnapshot(path string, start, end []byte) error {
	e.limiter.Wait(context.TODO())
	err := e.db.RawScanSaveRange(path, start, end, true)
	e.limiter.Release()

	return err
}

// ApplySnapshot apply a snapshort file from giving path
func (e *nemoDataEngine) ApplySnapshot(path string) error {
	e.limiter.Wait(context.TODO())
	err := e.db.IngestFile(path)
	e.limiter.Release()

	return err
}

func (e *nemoDataEngine) ScanIndexInfo(startKey []byte, endKey []byte, skipEmpty bool, handler func(key, idxInfo []byte) error) (cnt int, firstErr error) {
	var err error
	it := e.db.HmeataScan(startKey, endKey, false, skipEmpty)
	for ; it.Valid(); it.Next() {
		if err = handler(it.Key(), it.IndexInfo()); err != nil {
			if cnt == 0 {
				firstErr = err
			}
			cnt++
		}
	}
	it.Free()

	return
}

func (e *nemoDataEngine) SetIndexInfo(key, idxInfo []byte) error {
	e.limiter.Wait(context.TODO())
	err := e.db.HSetIndexInfo(key, idxInfo)
	e.limiter.Release()

	return err
}

func (e *nemoDataEngine) GetIndexInfo(key []byte) (idxInfo []byte, err error) {
	var ret int
	idxInfo, ret, err = e.db.HGetIndexInfo(key)
	if err == nil && ret < 0 {
		err = errors.Errorf("key %+v doesn't exist", key)
	}
	return
}
