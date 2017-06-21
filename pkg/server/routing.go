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

package server

import (
	"hash/crc32"
	"sync"

	"github.com/deepfabric/elasticell/pkg/util"
)

const (
	bucketSize = 128
	bucketM    = 127
)

type routingMap struct {
	sync.RWMutex
	m map[string]*session
}

func (m *routingMap) put(key string, value *session) {
	m.Lock()
	m.m[key] = value
	m.Unlock()
}

func (m *routingMap) delete(key string) *session {
	m.Lock()
	value := m.m[key]
	delete(m.m, key)
	m.Unlock()

	return value
}

type routing struct {
	rms []*routingMap
}

func newRouting() *routing {
	r := &routing{
		rms: make([]*routingMap, bucketSize, bucketSize),
	}

	for i := 0; i < bucketSize; i++ {
		r.rms[i] = &routingMap{
			m: make(map[string]*session),
		}
	}

	return r
}

func (r *routing) put(uuid []byte, value *session) {
	r.rms[getIndex(uuid)].put(util.SliceToString(uuid), value)
}

func (r *routing) delete(uuid []byte) *session {
	return r.rms[getIndex(uuid)].delete(util.SliceToString(uuid))
}

func getIndex(key []byte) int {
	return int(crc32.ChecksumIEEE(key) & bucketM)
}
