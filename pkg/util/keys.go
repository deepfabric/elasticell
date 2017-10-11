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

package util

import (
	"encoding/binary"
	"hash/crc64"

	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/fagongzi/goetty"
)

var (
	tab = crc64.MakeTable(crc64.ECMA)
	mp  = goetty.NewSyncPool(8, 16, 2)
)

// NoConvert no converter
func NoConvert(key []byte, do func([]byte) metapb.Cell) metapb.Cell {
	return do(key)
}

// Uint64Convert returns the hash crc64 result value, must use `ReleaseConvertBytes` to release
func Uint64Convert(key []byte, do func([]byte) metapb.Cell) metapb.Cell {
	b := mp.Alloc(8)
	binary.BigEndian.PutUint64(b, crc64.Checksum(key, tab))
	value := do(b)
	mp.Free(b)
	return value
}
