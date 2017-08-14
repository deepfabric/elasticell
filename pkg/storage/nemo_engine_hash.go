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
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	gonemo "github.com/deepfabric/go-nemo"
)

type nemoHashEngine struct {
	db *gonemo.NEMO
}

func newNemoHashEngine(db *gonemo.NEMO) HashEngine {
	return &nemoHashEngine{
		db: db,
	}
}

func (e *nemoHashEngine) HSet(key, field, value []byte) (int64, error) {
	n, err := e.db.HSet(key, field, value)
	return int64(n), err
}

func (e *nemoHashEngine) HGet(key, field []byte) ([]byte, error) {
	return e.db.HGet(key, field)
}

func (e *nemoHashEngine) HDel(key []byte, fields ...[]byte) (int64, error) {
	return e.db.HDel(key, fields...)
}

func (e *nemoHashEngine) HExists(key, field []byte) (bool, error) {
	return e.db.HExists(key, field)
}

func (e *nemoHashEngine) HKeys(key []byte) ([][]byte, error) {
	return e.db.HKeys(key)
}

func (e *nemoHashEngine) HVals(key []byte) ([][]byte, error) {
	return e.db.HVals(key)
}

func (e *nemoHashEngine) HGetAll(key []byte) ([]*raftcmdpb.FVPair, error) {
	fields, values, err := e.db.HGetall(key)
	if err != nil {
		return nil, err
	}

	if nil == fields || nil == values {
		return nil, nil
	}

	pairs := make([]*raftcmdpb.FVPair, len(fields))
	for idx, field := range fields {
		pairs[idx] = &raftcmdpb.FVPair{
			Field: field,
			Value: values[idx],
		}
	}

	return pairs, nil
}

func (e *nemoHashEngine) HLen(key []byte) (int64, error) {
	return e.db.HLen(key)
}

func (e *nemoHashEngine) HMGet(key []byte, fields ...[]byte) ([][]byte, []error) {
	values, errors := e.db.HMGet(key, fields)
	var errs []error
	if len(errors) > 0 {
		for _, err := range errors {
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	return values, errs
}

func (e *nemoHashEngine) HMSet(key []byte, fields, values [][]byte) error {
	_, err := e.db.HMSet(key, fields, values)
	return err
}

func (e *nemoHashEngine) HSetNX(key, field, value []byte) (int64, error) {
	return e.db.HSetnx(key, field, value)
}

func (e *nemoHashEngine) HStrLen(key, field []byte) (int64, error) {
	return e.db.HStrlen(key, field)
}

func (e *nemoHashEngine) HIncrBy(key, field []byte, incrment int64) ([]byte, error) {
	return e.db.HIncrby(key, field, incrment)
}
