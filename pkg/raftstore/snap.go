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

	writeBuff = 1024 * 1024 * 8 // 8mb
)

// SnapshotManager manager snapshot
type SnapshotManager interface {
	Register(key *mraft.SnapKey, step int) bool
	Deregister(key *mraft.SnapKey, step int)
	Create(snap *mraft.RaftSnapshotData) error
	Exists(key *mraft.SnapKey) bool
	WriteTo(key *mraft.SnapKey, conn goetty.IOSession) (uint64, error)
	CleanSnap(key *mraft.SnapKey) error
	ReceiveSnapData(data *mraft.SnapshotData) error
	ReceiveSnapDataComplete(data *mraft.SnapshotDataEnd) error
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

	if !exist(dir) {
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

func (m *defaultSnapshotManager) getTmpPathOfSnapKeyGZ(key *mraft.SnapKey) string {
	return fmt.Sprintf("%s.tmp", m.getPathOfSnapKey(key))
}

func (m *defaultSnapshotManager) Register(key *mraft.SnapKey, step int) bool {
	m.Lock()
	defer m.Unlock()

	fkey := formatKeyStep(key, step)

	if _, ok := m.registry[fkey]; ok {
		return false
	}

	m.registry[fkey] = emptyStruct
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
	gzPath := m.getPathOfSnapKeyGZ(&snap.Key)
	start := encStartKey(&snap.Cell)
	end := encEndKey(&snap.Cell)

	if !exist(gzPath) {
		if !exist(path) {
			err := m.db.CreateSnapshot(path, start, end)
			if err != nil {
				return errors.Wrapf(err, "")
			}
		}

		err := util.GZIP(path)
		if err != nil {
			return errors.Wrapf(err, "")
		}
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
	return exist(file)
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

func (m *defaultSnapshotManager) CleanSnap(key *mraft.SnapKey) error {
	var err error

	tmpFile := m.getTmpPathOfSnapKeyGZ(key)
	if exist(tmpFile) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap tmp file, file=<%s>, key=<%+v>",
			key.CellID,
			tmpFile,
			key)
		err = os.RemoveAll(tmpFile)
	}

	if err != nil {
		return err
	}

	file := m.getPathOfSnapKeyGZ(key)
	if exist(file) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap gz file, file=<%s>, key=<%+v>",
			key.CellID,
			file,
			key)
		err = os.RemoveAll(file)
	}

	if err != nil {
		return err
	}

	dir := m.getPathOfSnapKey(key)
	if exist(dir) {
		log.Infof("raftstore-snap[cell-%d]: delete exists snap dir, file=<%s>, key=<%+v>",
			key.CellID,
			dir,
			key)
		err = os.RemoveAll(dir)
	}

	return err
}

func (m *defaultSnapshotManager) ReceiveSnapDataComplete(data *mraft.SnapshotDataEnd) error {
	key := &data.Key

	file := m.getTmpPathOfSnapKeyGZ(key)
	if exist(file) {
		info, err := os.Stat(file)
		if err != nil {
			return errors.Wrapf(err, "")
		}

		if data.FileSize != uint64(info.Size()) {
			return fmt.Errorf("snap file size not match, got=<%d> expect=<%d> path=<%s>",
				info.Size(),
				data.FileSize,
				file)
		}

		return os.Rename(file, m.getPathOfSnapKeyGZ(key))
	}

	return fmt.Errorf("missing snapshot file, path=%s", file)
}

func (m *defaultSnapshotManager) ReceiveSnapData(data *mraft.SnapshotData) error {
	key := &data.Key

	file := m.getTmpPathOfSnapKeyGZ(key)
	var f *os.File
	var err error
	if exist(file) {
		f, err = os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			f.Close()
			return err
		}
	} else {
		f, err = os.Create(file)
		if err != nil {
			f.Close()
			return err
		}
	}

	n, err := f.Write(data.Data)
	if err != nil {
		f.Close()
		return err
	}

	if n != len(data.Data) {
		f.Close()
		return fmt.Errorf("write snapshot file failed, expect=<%d> actual=<%d>", len(data.Data), n)
	}

	f.Close()
	return nil
}

func (m *defaultSnapshotManager) Apply(key *mraft.SnapKey) error {
	file := m.getPathOfSnapKeyGZ(key)
	if !m.Exists(key) {
		return fmt.Errorf("missing snapshot file, path=%s", file)
	}

	defer m.CleanSnap(key)

	err := util.UnGZIP(file, m.dir)
	if err != nil {
		return err
	}
	defer os.RemoveAll(m.getPathOfSnapKey(key))

	return m.db.ApplySnapshot(m.getPathOfSnapKey(key))
}

func exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}
