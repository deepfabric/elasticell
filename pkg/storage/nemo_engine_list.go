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

type nemoListEngine struct {
	db *gonemo.NEMO
}

func newNemoListEngine(db *gonemo.NEMO) ListEngine {
	return &nemoListEngine{
		db: db,
	}
}

func (e *nemoListEngine) LIndex(key []byte, index int64) ([]byte, error) {
	return e.db.LIndex(key, index)
}

func (e *nemoListEngine) LInsert(key []byte, pos int, pivot []byte, value []byte) (int64, error) {
	return e.db.LInsert(key, pos, pivot, value)
}

func (e *nemoListEngine) LLen(key []byte) (int64, error) {
	return e.db.LLen(key)
}

func (e *nemoListEngine) LPop(key []byte) ([]byte, error) {
	return e.db.LPop(key)
}

func (e *nemoListEngine) LPush(key []byte, values ...[]byte) (int64, error) {
	// TODO: nemo must support more value push
	return e.db.LPush(key, values[0])
}

func (e *nemoListEngine) LPushX(key []byte, value []byte) (int64, error) {
	return e.db.LPushx(key, value)
}

func (e *nemoListEngine) LRange(key []byte, begin int64, end int64) ([][]byte, error) {
	_, values, err := e.db.LRange(key, begin, end)
	return values, err
}

func (e *nemoListEngine) LRem(key []byte, count int64, value []byte) (int64, error) {
	return e.db.LRem(key, count, value)
}

func (e *nemoListEngine) LSet(key []byte, index int64, value []byte) error {
	return e.db.LSet(key, index, value)
}

func (e *nemoListEngine) LTrim(key []byte, begin int64, end int64) error {
	return e.db.LTrim(key, begin, end)
}

func (e *nemoListEngine) RPop(key []byte) ([]byte, error) {
	return e.db.RPop(key)
}

func (e *nemoListEngine) RPush(key []byte, values ...[]byte) (int64, error) {
	// TODO: nemo must support more value push
	return e.db.RPush(key, values[0])
}

func (e *nemoListEngine) RPushX(key []byte, value []byte) (int64, error) {
	return e.db.RPushx(key, value)
}
