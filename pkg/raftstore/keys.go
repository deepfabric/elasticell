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
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
)

// for cell meta
const (
	cellStateSuffix = 0x01
)

const (
	// Following are the suffix after the local prefix.
	// For cell id
	raftLogSuffix    = 0x01
	raftStateSuffix  = 0x02
	applyStateSuffix = 0x03
	nextDocIDSuffix  = 0x04
)

// local is in (0x01, 0x02);
var (
	localPrefix byte = 0x01
	localMinKey      = []byte{localPrefix}
	localMaxKey      = []byte{localPrefix + 1}

	maxKey = []byte{}
	minKey = []byte{0xff}
)

var storeIdentKey = []byte{localPrefix, 0x01}

// data is in (z, z+1)
var (
	dataPrefix    byte = 'z'
	dataPrefixKey      = []byte{dataPrefix}
	dataMinKey         = []byte{dataPrefix}
	dataMaxKey         = []byte{dataPrefix + 1}

	dataPrefixKeySize = len(dataPrefixKey)
)

var (
	// We save two types region data in DB, for raft and other meta data.
	// When the store starts, we should iterate all region meta data to
	// construct peer, no need to travel large raft data, so we separate them
	// with different prefixes.
	cellRaftPrefix    byte = 0x02
	cellRaftPrefixKey      = []byte{localPrefix, cellRaftPrefix}
	cellMetaPrefix    byte = 0x03
	cellMetaPrefixKey      = []byte{localPrefix, cellMetaPrefix}
	cellMetaMinKey         = []byte{localPrefix, cellMetaPrefix}
	cellMetaMaxKey         = []byte{localPrefix, cellMetaPrefix + 1}

	// docID -> userKey
	cellDocIDPrefix    byte = 0x04
	cellDocIDPrefixKey      = []byte{localPrefix, cellDocIDPrefix}

	// index request queue key
	idxReqQueueKey = []byte{localPrefix, 0x05}
)

// GetStoreIdentKey return key of StoreIdent
func GetStoreIdentKey() []byte {
	return storeIdentKey
}

// GetMaxKey return max key
func GetMaxKey() []byte {
	return maxKey
}

// GetMinKey return min key
func GetMinKey() []byte {
	return minKey
}

func decodeCellMetaKey(key []byte) (uint64, byte, error) {
	prefixLen := len(cellMetaPrefixKey)
	keyLen := len(key)

	if prefixLen+9 != len(key) {
		return 0, 0, fmt.Errorf("invalid cell meta key length for key %v", key)
	}

	if !bytes.HasPrefix(key, cellMetaPrefixKey) {
		return 0, 0, fmt.Errorf("invalid region meta prefix for key %v", key)
	}

	return binary.BigEndian.Uint64(key[prefixLen:keyLen]), key[keyLen-1], nil
}

func getCellStateKey(cellID uint64) []byte {
	return getCellMetaKey(cellID, cellStateSuffix)
}

func getCellMetaKey(cellID uint64, suffix byte) []byte {
	buf := acquireBuf()
	buf.Write(cellMetaPrefixKey)
	buf.WriteInt64(int64(cellID))
	buf.WriteByte(suffix)
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func getCellMetaPrefix(cellID uint64) []byte {
	buf := acquireBuf()
	buf.Write(cellMetaPrefixKey)
	buf.WriteInt64(int64(cellID))
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func getDataKey(key []byte) []byte {
	buf := acquireBuf()
	data := getDataKey0(key, buf)
	releaseBuf(buf)
	return data
}

func getDataKey0(key []byte, buf *goetty.ByteBuf) []byte {
	buf.Write(dataPrefixKey)
	buf.Write(key)
	_, data, _ := buf.ReadBytes(buf.Readable())

	return data
}

func getOriginKey(key []byte) []byte {
	return key[len(dataPrefixKey):]
}

func getDataEndKey(endKey []byte) []byte {
	if len(endKey) == 0 {
		return dataMaxKey
	}
	return getDataKey(endKey)
}

// Get the `startKey` of current cell in encoded form.
func encStartKey(cell *metapb.Cell) []byte {
	// only initialized cell's startKey can be encoded, otherwise there must be bugs
	// somewhere.
	// cell
	if len(cell.Peers) == 0 {
		log.Fatalf("bug: cell peers len is empty")
	}

	return getDataKey(cell.Start)
}

/// Get the `endKey` of current region in encoded form.
func encEndKey(cell *metapb.Cell) []byte {
	// only initialized region's end_key can be encoded, otherwise there must be bugs
	// somewhere.
	if len(cell.Peers) == 0 {
		log.Fatalf("bug: cell peers len is empty")
	}
	return getDataEndKey(cell.End)
}

func getRaftStateKey(cellID uint64) []byte {
	return getCellIDKey(cellID, raftStateSuffix, 0, 0)
}

func getApplyStateKey(cellID uint64) []byte {
	return getCellIDKey(cellID, applyStateSuffix, 0, 0)
}

func getCellRaftPrefix(cellID uint64) []byte {
	buf := acquireBuf()
	buf.Write(cellRaftPrefixKey)
	buf.WriteInt64(int64(cellID))
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}

func getRaftLogKey(cellID uint64, logIndex uint64) []byte {
	return getCellIDKey(cellID, raftLogSuffix, 8, logIndex)
}

func getCellNextDocIDKey(cellID uint64) []byte {
	return getCellIDKey(cellID, nextDocIDSuffix, 0, 0)
}

func getDocIDKey(docID uint64) []byte {
	buf := acquireBuf()
	buf.Write(cellDocIDPrefixKey)
	buf.WriteInt64(int64(docID))
	_, data, _ := buf.ReadBytes(buf.Readable())
	releaseBuf(buf)
	return data
}

func getIdxReqQueueKey() []byte {
	return idxReqQueueKey
}

func getRaftLogIndex(key []byte) (uint64, error) {
	expectKeyLen := len(cellRaftPrefixKey) + 8*2 + 1
	if len(key) != expectKeyLen {
		return 0, fmt.Errorf("key<%v> is not a valid raft log key", key)
	}

	return binary.BigEndian.Uint64(key[len(cellRaftPrefixKey)+9:]), nil
}

func getCellIDKey(cellID uint64, suffix byte, extraCap int, extra uint64) []byte {
	buf := acquireBuf()
	buf.Write(cellRaftPrefixKey)
	buf.WriteInt64(int64(cellID))
	buf.WriteByte(suffix)
	if extraCap > 0 {
		buf.WriteInt64(int64(extra))
	}
	_, data, _ := buf.ReadBytes(buf.Readable())

	releaseBuf(buf)
	return data
}
