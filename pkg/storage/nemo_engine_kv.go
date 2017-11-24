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

type nemoKVEngine struct {
	limiter *util.Limiter
	db      *gonemo.NEMO
}

func newNemoKVEngine(db *gonemo.NEMO, cfg *NemoCfg) KVEngine {
	return &nemoKVEngine{
		limiter: util.NewLimiter(cfg.LimitConcurrencyWrite),
		db:      db,
	}
}

func (e *nemoKVEngine) Set(key, value []byte) error {
	e.limiter.Wait(context.TODO())
	err := e.db.Set(key, value, 0)
	e.limiter.Release()

	return err
}

func (e *nemoKVEngine) MSet(keys [][]byte, values [][]byte) error {
	e.limiter.Wait(context.TODO())
	err := e.db.MSet(keys, values)
	e.limiter.Release()

	return err
}

func (e *nemoKVEngine) Get(key []byte) ([]byte, error) {
	return e.db.Get(key)
}

func (e *nemoKVEngine) IncrBy(key []byte, incrment int64) (int64, error) {
	e.limiter.Wait(context.TODO())
	v, err := e.db.Incrby(key, incrment)
	e.limiter.Release()

	if err != nil {
		return 0, err
	}

	return util.StrInt64(v)
}

func (e *nemoKVEngine) DecrBy(key []byte, incrment int64) (int64, error) {
	e.limiter.Wait(context.TODO())
	v, err := e.db.Decrby(key, incrment)
	e.limiter.Release()

	if err != nil {
		return 0, err
	}

	return util.StrInt64(v)
}

func (e *nemoKVEngine) GetSet(key, value []byte) ([]byte, error) {
	e.limiter.Wait(context.TODO())
	value, err := e.db.GetSet(key, value, 0)
	e.limiter.Release()

	return value, err
}

func (e *nemoKVEngine) Append(key, value []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.Append(key, value)
	e.limiter.Release()

	return n, err
}

func (e *nemoKVEngine) SetNX(key, value []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.Setnx(key, value, 0)
	e.limiter.Release()

	return n, err
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
	e.limiter.Wait(context.TODO())
	err := e.db.BatchWrite(e.db.GetKvHandle(), nwb.wb, false)
	e.limiter.Release()

	return err
}
