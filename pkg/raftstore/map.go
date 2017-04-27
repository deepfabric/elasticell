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

import (
	"sync"

	"time"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
)

type cellPeersMap struct {
	sync.RWMutex
	m map[uint64]*PeerReplicate
}

func newCellPeersMap() *cellPeersMap {
	return &cellPeersMap{
		m: make(map[uint64]*PeerReplicate),
	}
}

func (m *cellPeersMap) size() uint32 {
	m.RLock()
	v := uint32(len(m.m))
	m.RUnlock()

	return v
}

func (m *cellPeersMap) put(key uint64, peers *PeerReplicate) {
	m.Lock()
	m.m[key] = peers
	m.Unlock()
}

func (m *cellPeersMap) get(key uint64) *PeerReplicate {
	m.RLock()
	v := m.m[key]
	m.RUnlock()

	return v
}

func (m *cellPeersMap) delete(key uint64) *PeerReplicate {
	m.Lock()
	v := m.m[key]
	delete(m.m, key)
	m.Unlock()

	return v
}

func (m *cellPeersMap) foreach(fn func(*PeerReplicate) (bool, error)) error {
	m.RLock()
	var err error
	var c bool
	for _, v := range m.m {
		c, err = fn(v)
		if err != nil || !c {
			break
		}
	}
	m.RUnlock()

	return err
}
func (m *cellPeersMap) values() []*PeerReplicate {
	m.RLock()
	var values []*PeerReplicate
	for _, v := range m.m {
		values = append(values, v)
	}
	m.RUnlock()

	return values
}

type peerCacheMap struct {
	sync.RWMutex
	m map[uint64]metapb.Peer
}

func newPeerCacheMap() *peerCacheMap {
	return &peerCacheMap{
		m: make(map[uint64]metapb.Peer),
	}
}

func (m *peerCacheMap) put(key uint64, peer metapb.Peer) {
	m.Lock()
	m.m[key] = peer
	m.Unlock()
}

func (m *peerCacheMap) get(key uint64) (metapb.Peer, bool) {
	m.RLock()
	v, ok := m.m[key]
	m.RUnlock()

	return v, ok
}

func (m *peerCacheMap) delete(key uint64) {
	m.Lock()
	delete(m.m, key)
	m.Unlock()
}

type applyDelegateMap struct {
	sync.RWMutex
	m map[uint64]*applyDelegate
}

func newApplyDelegateMap() *applyDelegateMap {
	return &applyDelegateMap{
		m: make(map[uint64]*applyDelegate),
	}
}

func (m *applyDelegateMap) put(key uint64, value *applyDelegate) *applyDelegate {
	m.Lock()
	old := m.m[key]
	m.m[key] = value
	m.Unlock()

	return old
}

func (m *applyDelegateMap) get(key uint64) *applyDelegate {
	m.RLock()
	v := m.m[key]
	m.RUnlock()

	return v
}

func (m *applyDelegateMap) delete(key uint64) *applyDelegate {
	m.Lock()
	v := m.m[key]
	delete(m.m, key)
	m.Unlock()

	return v
}

type peerHeartbeatsMap struct {
	sync.RWMutex
	m map[uint64]time.Time
}

func newPeerHeartbeatsMap() *peerHeartbeatsMap {
	return &peerHeartbeatsMap{
		m: make(map[uint64]time.Time, 3),
	}
}

func (m *peerHeartbeatsMap) clear() {
	m.Lock()
	for key := range m.m {
		delete(m.m, key)
	}
	m.Unlock()
}

func (m *peerHeartbeatsMap) has(key uint64) bool {
	m.Lock()
	_, ok := m.m[key]
	m.Unlock()

	return ok
}

func (m *peerHeartbeatsMap) put(key uint64, value time.Time) {
	m.Lock()
	m.m[key] = value
	m.Unlock()
}

func (m *peerHeartbeatsMap) putOnlyNotExist(key uint64, value time.Time) {
	m.Lock()
	_, ok := m.m[key]
	if !ok {
		m.m[key] = value
	}
	m.Unlock()
}

func (m *peerHeartbeatsMap) size() int {
	m.Lock()
	v := len(m.m)
	m.Unlock()

	return v
}

func (m *peerHeartbeatsMap) get(key uint64) time.Time {
	m.RLock()
	v := m.m[key]
	m.RUnlock()

	return v
}

func (m *peerHeartbeatsMap) delete(key uint64) {
	m.Lock()
	delete(m.m, key)
	m.Unlock()
}
