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

const (
	// KVType kv type
	KVType byte = 0x10
	// ListType list type
	ListType byte = 0x11
	// SetType set type
	SetType byte = 0x12
	// HashType hash type
	HashType byte = 0x13
)

func encodeKVKey(key []byte) []byte {
	ek := make([]byte, len(key)+1)
	ek[0] = KVType
	copy(ek[1:], key)
	return ek
}
