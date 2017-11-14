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
)

type nemoKVEngine struct {
	db *gonemo.NEMO
}

func newNemoKVEngine(db *gonemo.NEMO) KVEngine {
	return &nemoKVEngine{
		db: db,
	}
}

func (e *nemoKVEngine) Set(key, value []byte) error {
	return e.db.Set(key, value, 0)
}

func (e *nemoKVEngine) MSet(keys [][]byte, values [][]byte) error {
	return e.db.MSet(keys, values)
}

func (e *nemoKVEngine) Get(key []byte) ([]byte, error) {
	return e.db.Get(key)
}

func (e *nemoKVEngine) IncrBy(key []byte, incrment int64) (int64, error) {
	v, err := e.db.Incrby(key, incrment)
	if err != nil {
		return 0, err
	}

	return util.StrInt64(v)
}

func (e *nemoKVEngine) DecrBy(key []byte, incrment int64) (int64, error) {
	v, err := e.db.Decrby(key, incrment)
	if err != nil {
		return 0, err
	}

	return util.StrInt64(v)
}

func (e *nemoKVEngine) GetSet(key, value []byte) ([]byte, error) {
	return e.db.GetSet(key, value, 0)
}

func (e *nemoKVEngine) Append(key, value []byte) (int64, error) {
	return e.db.Append(key, value)
}

func (e *nemoKVEngine) SetNX(key, value []byte) (int64, error) {
	return e.db.Setnx(key, value, 0)
}

func (e *nemoKVEngine) StrLen(key []byte) (int64, error) {
	return e.db.StrLen(key)
}

func (e *nemoKVEngine) NewWriteBatch() WriteBatch {
	wb := gonemo.NewWriteBatch()
	return newNemoWriteBatch(wb)
}

func (e *nemoKVEngine) Write(wb WriteBatch) error {
	nwb := wb.(*nemoWriteBatch)
	return e.db.BatchWrite(e.db.GetKvHandle(), nwb.wb, false)
}
