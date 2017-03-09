package rocksdb

import (
	"github.com/deepfabric/elasticell/pkg/storage"
)

func (d *rocksdbDriver) Set(key []byte, value []byte) error {
	return d.engine.set(key, value)
}

func (d *rocksdbDriver) SetNX(key []byte, value []byte) (bool, error) {
	return false, nil
}

func (d *rocksdbDriver) SetRange(key []byte, value []byte, offset int) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) StrLen(key []byte) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) Get(keys ...[]byte) ([]byte, error) {
	for _, key := range keys {
		d.engine.get(key)
	}
	return nil, nil
}

func (d *rocksdbDriver) Append(key []byte, value []byte) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) Decr(key []byte) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) DecrBy(key []byte, decrement int64) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) Incr(key []byte) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) IncrBy(key []byte, decrement int64) (int64, error) {
	return 0, nil
}

func (d *rocksdbDriver) GetRange(key []byte, start int, end int) ([]byte, error) {
	return nil, nil
}

func (d *rocksdbDriver) GetSet(key []byte, value []byte) ([]byte, error) {
	return nil, nil
}

func (d *rocksdbDriver) MGet(keys ...[]byte) ([][]byte, error) {
	return nil, nil
}

func (d *rocksdbDriver) MSet(pairs ...[]*storage.KVPair) error {
	return nil
}

func (d *rocksdbDriver) MSetNX(pairs ...[]*storage.KVPair) error {
	return nil
}
