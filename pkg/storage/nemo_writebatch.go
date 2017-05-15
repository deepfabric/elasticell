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

// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	gonemo "github.com/deepfabric/go-nemo"
)

type nemoWriteBatch struct {
	wb *gonemo.WriteBatch
}

func newNemoWriteBatch(wb *gonemo.WriteBatch) WriteBatch {
	return &nemoWriteBatch{
		wb: wb,
	}
}

func (n *nemoWriteBatch) Delete(key []byte) error {
	// TODO: nemo bug, only one argment
	n.wb.WriteBatchDel(key, nil)
	return nil
}

func (n *nemoWriteBatch) Set(key []byte, value []byte) error {
	n.wb.WriteBatchPut(key, value)
	return nil
}
