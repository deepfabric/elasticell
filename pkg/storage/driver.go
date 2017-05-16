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

package storage

import "github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"

// WriteBatch batch operation
type WriteBatch interface {
	Delete(key []byte) error
	Set(key []byte, value []byte) error
}

// Driver is def storage interface
type Driver interface {
	GetEngine() Engine
	GetDataEngine() DataEngine

	// TODO: impl redis data struct engine
	GetKVEngine() KVEngine
	GetHashEngine() HashEngine
	// GetSetEngine() SetEngine
	// GetListEngine() ListEngine

	NewWriteBatch() WriteBatch
	Write(wb WriteBatch) error
}

// KVEngine is the storage of KV
type KVEngine interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	IncrBy(key []byte, incrment int64) (int64, error)
	DecrBy(key []byte, incrment int64) (int64, error)
	GetSet(key, value []byte) ([]byte, error)
	Append(key, value []byte) (int64, error)
	SetNX(key, value []byte) (int64, error)
	StrLen(key []byte) (int64, error)
}

// HashEngine is the storage of Hash
type HashEngine interface {
	HSet(key, field, value []byte) (int64, error)
	HGet(key, field []byte) ([]byte, error)
	HDel(key []byte, fields ...[]byte) (int64, error)
	HExists(key, field []byte) (bool, error)
	HKeys(key []byte) ([][]byte, error)
	HVals(key []byte) ([][]byte, error)
	HGetAll(key []byte) ([]*raftcmdpb.FVPair, error)
	HLen(key []byte) (int64, error)
	HMGet(key []byte, fields ...[]byte) ([][]byte, []error)
	HMSet(key []byte, fields, values [][]byte) error
	HSetNX(key, field, value []byte) (int64, error)
	HStrLen(key, field []byte) (int64, error)
	HIncrBy(key, field []byte, incrment int64) ([]byte, error)
}

// SetEngine is the storage of Set
type SetEngine interface {
}

// ListEngine is the storage of List
type ListEngine interface {
}

// DataEngine is the storage of redis data
type DataEngine interface {
	RangeDelete(start, end []byte) error
	// Scan scans the range and execute the handler fun.
	// returns false means end the scan.
	Scan(startKey []byte, endKey []byte, handler func(key, value []byte) (bool, error)) error
	// Scan scans the range and execute the handler fun.
	// returns false means end the scan.
	ScanSize(startKey []byte, endKey []byte, handler func(key []byte, size uint64) (bool, error)) error
	CompactRange(startKey []byte, endKey []byte) error
	// Seek the first key >= given key, if no found, return None.
	Seek(key []byte) ([]byte, []byte, error)
}

// Engine is the storage of meta data
type Engine interface {
	DataEngine

	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}
