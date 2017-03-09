package rocksdb

import (
	"github.com/deepfabric/elasticell/pkg/storage"
)

type rocksdbDriver struct {
	engine *RocksDB
}

// NewRocksDBStorageDriver create a rocksdb driver
func NewRocksDBStorageDriver(cfg *Config) (storage.Driver, error) {
	engine, err := OpenRocksDB(cfg)
	if err != nil {
		return nil, err
	}

	return &rocksdbDriver{
		engine: engine,
	}, nil
}
