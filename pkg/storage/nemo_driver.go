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

type nemoDrvier struct {
	db         *gonemo.NEMO
	metaEngine Engine
	dataEngine DataEngine
	kvEngine   KVEngine
	hashEngine HashEngine
	listEngine ListEngine
	setEngine  SetEngine
	zsetEngine ZSetEngine
}

// NewNemoDriver return a driver implemention by nemo
func NewNemoDriver(path string) (Driver, error) {
	opts := gonemo.NewDefaultOptions()
	db := gonemo.OpenNemo(opts, path)

	driver := &nemoDrvier{
		db: db,
	}

	driver.init()

	return driver, nil
}

func (n *nemoDrvier) init() {
	n.metaEngine = newNemoMetaEngine(n.db)
	n.dataEngine = newNemoDataEngine(n.db)
	n.kvEngine = newNemoKVEngine(n.db)
	n.hashEngine = newNemoHashEngine(n.db)
	n.listEngine = newNemoListEngine(n.db)
	n.setEngine = newNemoSetEngine(n.db)
	n.zsetEngine = newNemoZSetEngine(n.db)
}

func (n *nemoDrvier) GetEngine() Engine {
	return n.metaEngine
}

func (n *nemoDrvier) GetDataEngine() DataEngine {
	return n.dataEngine
}

func (n *nemoDrvier) GetKVEngine() KVEngine {
	return n.kvEngine
}

func (n *nemoDrvier) GetHashEngine() HashEngine {
	return n.hashEngine
}

func (n *nemoDrvier) GetListEngine() ListEngine {
	return n.listEngine
}

func (n *nemoDrvier) GetSetEngine() SetEngine {
	return n.setEngine
}

func (n *nemoDrvier) GetZSetEngine() ZSetEngine {
	return n.zsetEngine
}

func (n *nemoDrvier) NewWriteBatch() WriteBatch {
	wb := gonemo.NewWriteBatch()
	return newNemoWriteBatch(wb)
}

func (n *nemoDrvier) Write(wb WriteBatch) error {
	nwb := wb.(*nemoWriteBatch)
	// TODO: cfg
	return n.db.BatchWrite(n.db.GetMetaHandle(), nwb.wb, false)
}
