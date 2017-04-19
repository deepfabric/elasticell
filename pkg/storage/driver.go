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

// KVPair is a key and value pair
type KVPair struct {
	Key   []byte
	Value []byte
}

// Driver is def storage interface
type Driver interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	RangeDelete(start, end []byte) error
	// Scan scans the range and execute the handler fun.
	// returns true means end the scan.
	Scan(startKey []byte, endKey []byte, handler func(key, value []byte) (bool, error)) error
}
