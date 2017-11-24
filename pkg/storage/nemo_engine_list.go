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
	"golang.org/x/net/context"
)

type nemoListEngine struct {
	limiter *util.Limiter
	db      *gonemo.NEMO
}

func newNemoListEngine(db *gonemo.NEMO, cfg *NemoCfg) ListEngine {
	return &nemoListEngine{
		limiter: util.NewLimiter(cfg.LimitConcurrencyWrite),
		db:      db,
	}
}

func (e *nemoListEngine) LIndex(key []byte, index int64) ([]byte, error) {
	return e.db.LIndex(key, index)
}

func (e *nemoListEngine) LInsert(key []byte, pos int, pivot []byte, value []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.LInsert(key, pos, pivot, value)
	e.limiter.Release()

	return n, err
}

func (e *nemoListEngine) LLen(key []byte) (int64, error) {
	return e.db.LLen(key)
}

func (e *nemoListEngine) LPop(key []byte) ([]byte, error) {
	e.limiter.Wait(context.TODO())
	value, err := e.db.LPop(key)
	e.limiter.Release()

	return value, err
}

func (e *nemoListEngine) LPush(key []byte, values ...[]byte) (int64, error) {
	// TODO: nemo must support more value push
	e.limiter.Wait(context.TODO())
	n, err := e.db.LPush(key, values[0])
	e.limiter.Release()

	return n, err
}

func (e *nemoListEngine) LPushX(key []byte, value []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.LPushx(key, value)
	e.limiter.Release()

	return n, err
}

func (e *nemoListEngine) LRange(key []byte, begin int64, end int64) ([][]byte, error) {
	_, values, err := e.db.LRange(key, begin, end)
	return values, err
}

func (e *nemoListEngine) LRem(key []byte, count int64, value []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.LRem(key, count, value)
	e.limiter.Release()

	return n, err
}

func (e *nemoListEngine) LSet(key []byte, index int64, value []byte) error {
	e.limiter.Wait(context.TODO())
	err := e.db.LSet(key, index, value)
	e.limiter.Release()

	return err
}

func (e *nemoListEngine) LTrim(key []byte, begin int64, end int64) error {
	e.limiter.Wait(context.TODO())
	err := e.db.LTrim(key, begin, end)
	e.limiter.Release()

	return err
}

func (e *nemoListEngine) RPop(key []byte) ([]byte, error) {
	e.limiter.Wait(context.TODO())
	value, err := e.db.RPop(key)
	e.limiter.Release()

	return value, err
}

func (e *nemoListEngine) RPush(key []byte, values ...[]byte) (int64, error) {
	// TODO: nemo must support more value push
	e.limiter.Wait(context.TODO())
	n, err := e.db.RPush(key, values[0])
	e.limiter.Release()

	return n, err
}

func (e *nemoListEngine) RPushX(key []byte, value []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.RPushx(key, value)
	e.limiter.Release()

	return n, err
}
