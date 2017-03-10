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

package rocksdb

import (
	"errors"
	"os"

	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/tecbot/gorocksdb"
)

const (
	defaultBlockSize             = 1024 * 64
	defaultLRUCacheSize          = 1024 * 1024 * 1024
	defaultBloomFilterBitsPerKey = 10

	// defaultWriteBufferSize default write buffer size
	defaultWriteBufferSize                = 1024 * 1024 * 128
	defaultMaxWriteBufferNumber           = 8
	defaultLevel0FileNumCompactionTrigger = 4
	defaultMaxBytesForLevelBase           = 1024 * 1024 * 1024 * 2
	defaultMinWriteBufferNumberToMerge    = 2
	defaultTargetFileSizeBase             = 1024 * 1024 * 128
	defaultMaxBackgroundFlushes           = 2
	defaultMaxBackgroundCompactions       = 4
	defaultMinLevelToCompress             = 3
	defaultMaxLogFileSize                 = 1024 * 1024 * 32
	defaultLogFileTimeToRoll              = 3600 * 24 * 3
)

var (
	// ErrNoneDataDir data dir not set
	ErrNoneDataDir = errors.New("data dir not set")
)

// Config rocksdb config used for create
type Config struct {
	DataDir string

	BlockSize             int
	LRUCacheSize          int
	BloomFilterBitsPerKey int

	EnableLRUCache bool
	// EnableCacheIndexAndFilterBlocks bool
	EnableVerifyChecksums bool
	EnableStatistics      bool

	WriteBufferSize                int
	MaxWriteBufferNumber           int
	Level0FileNumCompactionTrigger int
	MaxBytesForLevelBase           uint64
	MinWriteBufferNumberToMerge    int
	TargetFileSizeBase             uint64
	MaxBackgroundFlushes           int
	MaxBackgroundCompactions       int
	MinLevelToCompress             int
	MaxLogFileSize                 int
	LogFileTimeToRoll              int
}

// RocksDB rocksdb wrapper
type RocksDB struct {
	cfg                 *Config
	db                  *gorocksdb.DB
	dbOptions           *gorocksdb.Options
	defaultReadOptions  *gorocksdb.ReadOptions
	defaultWriteOptions *gorocksdb.WriteOptions
	wb                  *gorocksdb.WriteBatch
}

// OpenRocksDB open a rocksdb instance
func OpenRocksDB(cfg *Config) (*RocksDB, error) {
	if len(cfg.DataDir) == 0 {
		return nil, ErrNoneDataDir
	}

	os.MkdirAll(cfg.DataDir, 0755)

	// options need be adjust due to using hdd or sdd, please reference
	// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()

	// use large block to reduce index block size for hdd
	// if using ssd, should use the default value
	bbto.SetBlockSize(util.GetIntValue(cfg.BlockSize, defaultBlockSize))

	// should about 20% less than host RAM
	// http://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html
	if cfg.EnableLRUCache {
		bbto.SetBlockCache(gorocksdb.NewLRUCache(util.GetIntValue(cfg.LRUCacheSize, defaultLRUCacheSize)))
	}

	// TODO: use absolute8511's repository: https://github.com/absolute8511/gorocksdb
	// for hdd , we nee cache index and filter blocks
	// if cfg.EnableCacheIndexAndFilterBlocks {
	// 	bbto.SetCacheIndexAndFilterBlocks(true)
	// }

	filter := gorocksdb.NewBloomFilter(util.GetIntValue(cfg.BloomFilterBitsPerKey, defaultBloomFilterBitsPerKey))
	bbto.SetFilterPolicy(filter)

	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(-1)
	// keep level0_file_num_compaction_trigger * write_buffer_size = max_bytes_for_level_base to minimize write amplification
	opts.SetWriteBufferSize(util.GetIntValue(cfg.WriteBufferSize, defaultWriteBufferSize))
	opts.SetMaxWriteBufferNumber(util.GetIntValue(cfg.MaxWriteBufferNumber, defaultMaxWriteBufferNumber))
	opts.SetLevel0FileNumCompactionTrigger(util.GetIntValue(cfg.Level0FileNumCompactionTrigger, defaultLevel0FileNumCompactionTrigger))
	opts.SetMaxBytesForLevelBase(util.GetUint64Value(cfg.MaxBytesForLevelBase, defaultMaxBytesForLevelBase))
	opts.SetMinWriteBufferNumberToMerge(util.GetIntValue(cfg.MinWriteBufferNumberToMerge, defaultMinWriteBufferNumberToMerge))
	opts.SetTargetFileSizeBase(util.GetUint64Value(cfg.TargetFileSizeBase, defaultTargetFileSizeBase))
	opts.SetMaxBackgroundFlushes(util.GetIntValue(cfg.MaxBackgroundFlushes, defaultMaxBackgroundFlushes))
	opts.SetMaxBackgroundCompactions(util.GetIntValue(cfg.MaxBackgroundCompactions, defaultMaxBackgroundCompactions))
	opts.SetMinLevelToCompress(util.GetIntValue(cfg.MinLevelToCompress, defaultMinLevelToCompress))
	// we use table, so we use prefix seek feature
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
	// TODO: use absolute8511's repository: https://github.com/absolute8511/gorocksdb
	// opts.SetMemtablePrefixBloomSizeRatio(0.1)
	if cfg.EnableStatistics {
		opts.EnableStatistics()
	}
	opts.SetMaxLogFileSize(util.GetIntValue(cfg.MaxLogFileSize, defaultMaxLogFileSize))
	opts.SetLogFileTimeToRoll(util.GetIntValue(cfg.LogFileTimeToRoll, defaultLogFileTimeToRoll))
	rdb := &RocksDB{
		cfg:                 cfg,
		dbOptions:           opts,
		defaultReadOptions:  gorocksdb.NewDefaultReadOptions(),
		defaultWriteOptions: gorocksdb.NewDefaultWriteOptions(),
		wb:                  gorocksdb.NewWriteBatch(),
	}

	rdb.defaultReadOptions.SetVerifyChecksums(cfg.EnableVerifyChecksums)

	db, err := gorocksdb.OpenDb(opts, cfg.DataDir)
	if err != nil {
		return nil, err
	}

	rdb.db = db

	return rdb, nil
}

func (rdb *RocksDB) set(key []byte, value []byte) error {
	rdb.wb.Clear()
	rdb.wb.Put(key, value)
	return rdb.db.Write(rdb.defaultWriteOptions, rdb.wb)
}

func (rdb *RocksDB) get(key []byte) ([]byte, error) {
	return rdb.db.GetBytes(rdb.defaultReadOptions, key)
}
