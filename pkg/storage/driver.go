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

// Kind is the type for engine
type Kind int

var (
	// Meta is used for meta info storage
	Meta = Kind(0)
	// DataKV is used for KV storage
	DataKV = Kind(1)
	// DataHash is used for Hash storage
	DataHash = Kind(2)
	// DataSet is used for Set storage
	DataSet = Kind(4)
	// DataZSet is used for ZSet storage
	DataZSet = Kind(8)
	// DataList is used for List storage
	DataList = Kind(16)
	// Data is used for DATA storage
	Data = Kind(DataKV | DataHash | DataSet | DataZSet | DataList)
)

// WriteBatch batch operation
type WriteBatch interface {
	Delete(kind Kind, key []byte) error
	Set(kind Kind, key []byte, value []byte) error
}

// Driver is def storage interface
type Driver interface {
	GetEngine(kind Kind) Engine
	NewWriteBatch() WriteBatch
	Write(wb WriteBatch) error
}

// Engine is the data storage engine
type Engine interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	RangeDelete(start, end []byte) error
	// Scan scans the range and execute the handler fun.
	// returns false means end the scan.
	Scan(startKey []byte, endKey []byte, handler func(key, value []byte) (bool, error)) error
	CompactRange(startKey []byte, endKey []byte) error
	// Seek the first key >= given key, if no found, return None.
	Seek(key []byte) ([]byte, []byte, error)
}
