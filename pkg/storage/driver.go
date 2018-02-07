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
	GetKVEngine() KVEngine
	GetHashEngine() HashEngine
	GetSetEngine() SetEngine
	GetZSetEngine() ZSetEngine
	GetListEngine() ListEngine
	NewWriteBatch() WriteBatch
	Write(wb WriteBatch, sync bool) error
}

// KVEngine is the storage of KV
type KVEngine interface {
	Set(key, value []byte) error
	MSet(keys [][]byte, values [][]byte) error
	Get(key []byte) ([]byte, error)
	IncrBy(key []byte, incrment int64) (int64, error)
	DecrBy(key []byte, incrment int64) (int64, error)
	GetSet(key, value []byte) ([]byte, error)
	Append(key, value []byte) (int64, error)
	SetNX(key, value []byte) (int64, error)
	StrLen(key []byte) (int64, error)
	NewWriteBatch() WriteBatch
	Write(wb WriteBatch) error
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
	SAdd(key []byte, members ...[]byte) (int64, error)
	SRem(key []byte, members ...[]byte) (int64, error)
	SCard(key []byte) (int64, error)
	SMembers(key []byte) ([][]byte, error)
	SIsMember(key []byte, member []byte) (int64, error)
	SPop(key []byte) ([]byte, error)
}

// ZSetEngine is the storage of ZSet
type ZSetEngine interface {
	ZAdd(key []byte, score float64, member []byte) (int64, error)
	ZCard(key []byte) (int64, error)
	ZCount(key []byte, min []byte, max []byte) (int64, error)
	ZIncrBy(key []byte, member []byte, by float64) ([]byte, error)
	ZLexCount(key []byte, min []byte, max []byte) (int64, error)
	ZRange(key []byte, start int64, stop int64) ([]*raftcmdpb.ScorePair, error)
	ZRangeByLex(key []byte, min []byte, max []byte) ([][]byte, error)
	ZRangeByScore(key []byte, min []byte, max []byte) ([]*raftcmdpb.ScorePair, error)
	ZRank(key []byte, member []byte) (int64, error)
	ZRem(key []byte, members ...[]byte) (int64, error)
	ZRemRangeByLex(key []byte, min []byte, max []byte) (int64, error)
	ZRemRangeByRank(key []byte, start int64, stop int64) (int64, error)
	ZRemRangeByScore(key []byte, min []byte, max []byte) (int64, error)
	ZScore(key []byte, member []byte) ([]byte, error)
}

// ListEngine is the storage of List
type ListEngine interface {
	LIndex(key []byte, index int64) ([]byte, error)
	LInsert(key []byte, pos int, pivot []byte, value []byte) (int64, error)
	LLen(key []byte) (int64, error)
	LPop(key []byte) ([]byte, error)
	LPush(key []byte, values ...[]byte) (int64, error)
	LPushX(key []byte, value []byte) (int64, error)
	LRange(key []byte, begin int64, end int64) ([][]byte, error)
	LRem(key []byte, count int64, value []byte) (int64, error)
	LSet(key []byte, index int64, value []byte) error
	LTrim(key []byte, begin int64, end int64) error
	RPop(key []byte) ([]byte, error)
	RPush(key []byte, values ...[]byte) (int64, error)
	RPushX(key []byte, value []byte) (int64, error)
}

// Seekable support seek
type Seekable interface {
	Seek(key []byte) ([]byte, []byte, error)
}

// Scanable support scan
type Scanable interface {
	// Scan scans the range and execute the handler fun.
	// returns false means end the scan.
	Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error
	// Free free the pooled bytes
	Free(pooled []byte)
}

// RangeDeleteable support range delete
type RangeDeleteable interface {
	RangeDelete(start, end []byte) error
}

// DataEngine is the storage of redis data
type DataEngine interface {
	RangeDeleteable
	// GetTargetSizeKey Find a key in the range [startKey, endKey) that sum size over target
	// if found returns the key
	GetTargetSizeKey(startKey []byte, endKey []byte, size uint64) (uint64, []byte, error)
	// CreateSnapshot create a snapshot file under the giving path
	CreateSnapshot(path string, start, end []byte) error
	// ApplySnapshot apply a snapshort file from giving path
	ApplySnapshot(path string) error

	// ScanIndexInfo scans the range and execute the handler fun. Returens a tuple (error count, first error)
	ScanIndexInfo(startKey []byte, endKey []byte, skipEmpty bool, handler func(key, idxInfo []byte) error) (int, error)
	SetIndexInfo(key, idxInfo []byte) error
	GetIndexInfo(key []byte) (idxInfo []byte, err error)
}

// Engine is the storage of meta data
type Engine interface {
	Scanable
	Seekable
	RangeDeleteable

	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}
