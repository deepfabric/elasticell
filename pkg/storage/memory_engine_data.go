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

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

type memoryDataEngine struct {
	kv *util.KVTree
}

func newMemoryDataEngine(kv *util.KVTree) DataEngine {
	return &memoryDataEngine{
		kv: kv,
	}
}

func (e *memoryDataEngine) RangeDelete(start, end []byte) error {
	e.kv.RangeDelete(start, end)
	return nil
}

func (e *memoryDataEngine) GetTargetSizeKey(startKey []byte, endKey []byte, size uint64) (uint64, []byte, error) {
	return 0, nil, nil
}

func (e *memoryDataEngine) CreateSnapshot(path string, start, end []byte) error {
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return nil
	}

	f, err := os.Create(fmt.Sprintf("%s/data.sst", path))
	if err != nil {
		return err
	}
	defer f.Close()

	return e.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		_, err := f.Write(goetty.Int2Bytes(len(key)))
		if err != nil {
			return false, nil
		}

		_, err = f.Write(goetty.Int2Bytes(len(value)))
		if err != nil {
			return false, nil
		}

		_, err = f.Write(key)
		if err != nil {
			return false, nil
		}

		_, err = f.Write(value)
		if err != nil {
			return false, nil
		}

		return true, nil
	})
}

func (e *memoryDataEngine) ApplySnapshot(path string) error {
	data, err := ioutil.ReadFile(fmt.Sprintf("%s/data.sst", path))
	if err != nil {
		return err
	}

	if len(data) == 0 {
		return nil
	}

	buf := goetty.NewByteBuf(len(data))
	buf.Write(data)

	for {
		if buf.Readable() > 8 {
			_, v, err := buf.ReadBytes(4)
			if err != nil {
				return err
			}

			keySize := goetty.Byte2Int(v)

			_, v, err = buf.ReadBytes(4)
			if err != nil {
				return err
			}

			valueSize := goetty.Byte2Int(v)

			_, key, err := buf.ReadBytes(keySize)
			if err != nil {
				return err
			}

			_, value, err := buf.ReadBytes(valueSize)
			if err != nil {
				return err
			}

			e.kv.Put(key, value)
		} else {
			break
		}
	}

	buf.Release()
	return nil
}
