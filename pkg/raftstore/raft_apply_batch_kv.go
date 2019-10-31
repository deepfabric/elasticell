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

package raftstore

type redisKVBatch struct {
	kvKeys   [][]byte
	kvValues [][]byte
}

func (rb *redisKVBatch) set(key, value []byte) {
	rb.kvKeys = append(rb.kvKeys, key)
	rb.kvValues = append(rb.kvValues, value)
}

func (rb *redisKVBatch) hasSetBatch() bool {
	return len(rb.kvKeys) > 0
}

func (rb *redisKVBatch) reset() {
	rb.kvKeys = rb.kvKeys[:0]
	rb.kvValues = rb.kvValues[:0]
}
