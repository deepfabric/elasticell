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
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
)

var (
	creating = 1
	sending  = 2

	writeBuff = 1024 * 1024 // 1mb
)

// SnapshotManager manager snapshot
type SnapshotManager interface {
	Register(key *mraft.SnapKey, step int) bool
	Deregister(key *mraft.SnapKey, step int)
	Create(snap *mraft.RaftSnapshotData) error
	Exists(key *mraft.SnapKey) bool
	WriteTo(key *mraft.SnapKey, conn goetty.IOSession) (uint64, error)
	WriteSnapData(data *mraft.SnapshotData) error
	Apply(key *mraft.SnapKey) error
}

type defaultSnapshotManager struct {
	sync.RWMutex

	cfg *Cfg
	db  storage.DataEngine
	dir string

	registry map[string]struct{}
}

func newDefaultSnapshotManager(cfg *Cfg, db storage.DataEngine) SnapshotManager {
	dir := cfg.getSnapDir()

	if !fileutil.Exist(dir) {
		if err := os.Mkdir(dir, 0750); err != nil {
			log.Fatalf("raftstore-snap: cannot create dir for snapshot, errors:\n %+v",
				err)
		}
	}

	return &defaultSnapshotManager{
		dir:      dir,
		db:       db,
		registry: make(map[string]struct{}),
	}
}

func formatKey(key *mraft.SnapKey) string {
	return fmt.Sprintf("%d_%d_%d", key.CellID, key.Term, key.Index)
}

func formatKeyStep(key *mraft.SnapKey, step int) string {
	return fmt.Sprintf("%s_%d", formatKey(key), step)
}

func (m *defaultSnapshotManager) getPathOfSnapKey(key *mraft.SnapKey) string {
	return fmt.Sprintf("%s/%s", m.dir, formatKey(key))
}

func (m *defaultSnapshotManager) getPathOfSnapKeyGZ(key *mraft.SnapKey) string {
	return fmt.Sprintf("%s.gz", m.getPathOfSnapKey(key))
}

func (m *defaultSnapshotManager) Register(key *mraft.SnapKey, step int) bool {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(key, step)

	if _, ok := m.registry[fkey]; ok {
		return false
	}

	m.registry[fkey] = struct{}{}
	return true
}

func (m *defaultSnapshotManager) Deregister(key *mraft.SnapKey, step int) {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(key, step)
	delete(m.registry, fkey)
}

func (m *defaultSnapshotManager) inRegistry(key *mraft.SnapKey, step int) bool {
	m.RLock()
	defer m.RUnlock()

	fkey := formatKeyStep(key, step)
	_, ok := m.registry[fkey]

	return ok
}

func (m *defaultSnapshotManager) Create(snap *mraft.RaftSnapshotData) error {
	path := m.getPathOfSnapKey(&snap.Key)
	start := encStartKey(&snap.Cell)
	end := encEndKey(&snap.Cell)

	if fileutil.Exist(path) {
		if !m.inRegistry(&snap.Key, sending) {
			err := os.RemoveAll(path)
			if err != nil {
				return errors.Wrapf(err, "remove exists snap dir: %s", path)
			}

			log.Debugf("raftstore-snap[cell-%d]: remove exists old snap data, key=<%+v> path=<%s>",
				snap.Key.CellID,
				snap,
				path)
		} else {
			return fmt.Errorf("create snapshot file path=<%s> already exists", path)
		}
	}

	err := m.db.CreateSnapshot(path, start, end)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	err = util.GZIP(path)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	info, err := os.Stat(fmt.Sprintf("%s.gz", path))
	if err != nil {
		return errors.Wrapf(err, "")
	}

	snap.FileSize = uint64(info.Size())
	return nil
}

func (m *defaultSnapshotManager) Exists(key *mraft.SnapKey) bool {
	file := m.getPathOfSnapKeyGZ(key)
	return fileutil.Exist(file)
}

func (m *defaultSnapshotManager) WriteTo(key *mraft.SnapKey, conn goetty.IOSession) (uint64, error) {
	file := m.getPathOfSnapKeyGZ(key)

	if !m.Exists(key) {
		return 0, fmt.Errorf("missing snapshot file: %s", file)
	}

	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var written int64
	buf := make([]byte, writeBuff)

	for {
		nr, er := f.Read(buf)
		if nr > 0 {
			written += int64(nr)

			dst := &mraft.SnapshotData{}
			dst.Key = *key
			dst.Data = buf[0:nr]

			err := conn.Write(dst)
			if err != nil {
				return 0, err
			}
		}
		if er != nil {
			if er != io.EOF {
				return 0, er
			}
			break
		}
	}

	return uint64(written), nil
}

func (m *defaultSnapshotManager) WriteSnapData(data *mraft.SnapshotData) error {
	key := &data.Key

	file := m.getPathOfSnapKeyGZ(key)
	var f *os.File
	var err error
	if m.Exists(key) {
		f, err = os.OpenFile(file, os.O_APPEND, 0)
		if err != nil {
			return err
		}
	} else {
		f, err = os.Create(file)
		if err != nil {
			return err
		}
	}

	defer f.Close()
	n, err := f.Write(data.Data)
	if err != nil {
		return err
	}

	if n != len(data.Data) {
		return fmt.Errorf("write snapshot file failed, expect=<%d> actual=<%d>", len(data.Data), n)
	}

	return nil
}

func (m *defaultSnapshotManager) Apply(key *mraft.SnapKey) error {
	file := m.getPathOfSnapKeyGZ(key)
	defer os.RemoveAll(file)

	if !m.Exists(key) {
		return fmt.Errorf("missing snapshot file, path=%s", file)
	}

	err := util.UnGZIP(file, m.dir)
	if err != nil {
		return err
	}

	return m.db.ApplySnapshot(m.getPathOfSnapKey(key))
}
