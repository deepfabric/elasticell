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

func (m *cellPeersMap) delete(key uint64) {
	m.Lock()
	delete(m.m, key)
	m.Unlock()
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

func (m *peerCacheMap) get(key uint64) metapb.Peer {
	m.RLock()
	v := m.m[key]
	m.RUnlock()

	return v
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

func (m *applyDelegateMap) delete(key uint64) {
	m.Lock()
	delete(m.m, key)
	m.Unlock()
}
