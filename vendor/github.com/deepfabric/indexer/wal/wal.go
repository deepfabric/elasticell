// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/deepfabric/indexer/wal/walpb"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	metadataType int64 = iota + 1
	entryType
	stateType
	crcType
	snapshotType

	// warnSyncDuration is the amount of time allotted to an fsync before
	// logging a warning
	warnSyncDuration = time.Second
)

var (
	// SegmentSizeBytes is the preallocated size of each wal segment file.
	// The actual size might be larger than this. In general, the default
	// value should be used, but this is defined as an exported variable
	// so that tests can set a different segment size.
	SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

	ErrFileNotFound = errors.New("wal: file not found")
	ErrCRCMismatch  = errors.New("wal: crc mismatch")
	crcTable        = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	dir string // the living directory of the underlay files

	// dirFile is a fd for the wal directory for syncing on Rename
	dirFile *os.File

	start     walpb.Snapshot // snapshot to start reading
	decoder   *decoder       // decoder to decode records
	readClose func() error   // closer for decode reader

	mu      sync.Mutex
	enti    uint64   // index of the last entry saved to the wal
	encoder *encoder // encoder to encode records

	tail     *os.File //the tail segment
	walNames []string // the segment files the WAL holds (the name is increasing)
	fp       *filePipeline
}

// Create creates a WAL ready for appending records.
func Create(dirpath string) (*WAL, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	// keep temporary wal directory so WAL initialization appears atomic
	tmpdirpath := filepath.Clean(dirpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}

	p := filepath.Join(tmpdirpath, walName(0, 0))
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:      dirpath,
		walNames: make([]string, 0),
	}
	if w, err = w.renameWal(tmpdirpath); err != nil {
		return nil, err
	}

	// directory was renamed; sync parent dir to persist rename
	pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
	if perr != nil {
		return nil, perr
	}
	if perr = fileutil.Fsync(pdir); perr != nil {
		return nil, perr
	}
	if perr = pdir.Close(); err != nil {
		return nil, perr
	}

	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	w.tail = f.File
	w.walNames = append(w.walNames, filepath.Join(dirpath, walName(0, 0)))
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *WAL) renameWal(tmpdirpath string) (*WAL, error) {
	if err := os.RemoveAll(w.dir); err != nil {
		return nil, err
	}
	// On non-Windows platforms, hold the lock while renaming. Releasing
	// the lock and trying to reacquire it quickly can be flaky because
	// it's possible the process will fork to spawn a process while this is
	// happening. The fds are set up as close-on-exec by the Go runtime,
	// but there is a window between the fork and the exec where another
	// process holds the lock.
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		if _, ok := err.(*os.LinkError); ok {
			return w.renameWalUnlock(tmpdirpath)
		}
		return nil, err
	}
	w.fp = newFilePipeline(w.dir, SegmentSizeBytes)
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df
	return w, err
}

func (w *WAL) renameWalUnlock(tmpdirpath string) (*WAL, error) {
	// rename of directory with locked files doesn't work on windows/cifs;
	// close the WAL to release the locks so the directory can be renamed.
	log.Infof("releasing file lock to rename %q to %q", tmpdirpath, w.dir)
	w.Close(false)
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}
	// reopen and relock
	newWAL, oerr := Open(w.dir, walpb.Snapshot{})
	if oerr != nil {
		return nil, oerr
	}
	if _, err := newWAL.ReadAll(); err != nil {
		newWAL.Close(false)
		return nil, err
	}
	return newWAL, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
func Open(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(dirpath, snap, false)
}

func openAtIndex(dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	names, err := readWalNames(dirpath)
	if err != nil {
		return nil, err
	}

	nameIndex, ok := searchIndex(names, snap.Index)
	if !ok || !isValidSeq(names[nameIndex:]) {
		return nil, ErrFileNotFound
	}

	// open the wal files
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	walNames := make([]string, 0)
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		if write {
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}
			rcs = append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
		walNames = append(walNames, p)
	}

	closer := func() error { return closeAll(rcs...) }

	// create a WAL ready for reading
	w := &WAL{
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		walNames:  walNames,
	}

	if write {
		w.fp = newFilePipeline(w.dir, SegmentSizeBytes)
	}

	return w, nil
}

// OpenAtBeginning opens the WAL at the beginning.
// The WAL cannot be appended to before reading out all of its
// previous records.
func OpenAtBeginning(dirpath string) (*WAL, error) {
	names, err := readWalNames(dirpath)
	if err != nil && errors.Cause(err) != ErrFileNotFound {
		return nil, err
	}
	nameIndex := 0

	// open the wal files
	rcs := make([]io.ReadCloser, 0)
	rs := make([]io.Reader, 0)
	walNames := make([]string, 0)
	for _, name := range names[nameIndex:] {
		p := filepath.Join(dirpath, name)
		l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
		if err != nil {
			closeAll(rcs...)
			return nil, errors.Wrap(err, "")
		}
		rcs = append(rcs, l)
		rs = append(rs, rcs[len(rcs)-1])
		walNames = append(walNames, p)
	}

	closer := func() error { return closeAll(rcs...) }

	// create a WAL ready for reading
	w := &WAL{
		dir:       dirpath,
		start:     walpb.Snapshot{},
		decoder:   newDecoder(rs...),
		readClose: closer,
		walNames:  walNames,
	}

	w.fp = newFilePipeline(w.dir, SegmentSizeBytes)
	if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
		return nil, errors.Wrap(err, "")
	}
	return w, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
func (w *WAL) ReadAll() (ents []walpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}
	decoder := w.decoder

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			if e.Index > w.start.Index {
				ents = append(ents[:e.Index-w.start.Index-1], e)
			}
			w.enti = e.Index
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				err = errors.Wrap(ErrCRCMismatch, "")
				return
			}
			decoder.updateCRC(rec.Crc)
		default:
		}
	}
	cause := errors.Cause(err)
	switch w.fp {
	case nil:
		// We do not have to read out all entries in read mode.
		// The last record maybe a partial written one, so
		// ErrunexpectedEOF might be returned.
		if cause != io.EOF && cause != io.ErrUnexpectedEOF {
			return
		}
	default:
		// We must read all of the entries if WAL is opened in write mode.
		if cause != io.EOF {
			return
		}
	}
	err = nil

	if w.fp != nil {
		if err = w.advance(w.decoder.lastCRC()); err != nil {
			return
		}
	}
	// close decoder, disable reading
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.decoder = nil
	return
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
func (w *WAL) cut() error {
	// close old wal file; truncate to avoid wasting space if an early cut
	off, serr := w.tail.Seek(0, io.SeekCurrent)
	if serr != nil {
		return serr
	}
	if err := w.tail.Truncate(off); err != nil {
		return err
	}
	if err := w.Sync(); err != nil {
		return err
	}

	return w.advance(w.encoder.crc.Sum32())
}

// CompactAll remove all entries.
func (w *WAL) CompactAll() (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err = w.clean(); err != nil {
		return
	}

	err = w.advance(w.encoder.crc.Sum32())
	return
}

// CompactAll remove all entries.
func (w *WAL) clean() (err error) {
	for _, name := range w.walNames {
		if err = os.Remove(name); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	w.walNames = w.walNames[0:0]
	return
}

func (w *WAL) advance(prevCrc uint32) (err error) {
	if w.tail != nil {
		if err = w.tail.Close(); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		w.tail = nil
	}
	fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

	// create a temp wal file with name sequence + 1, or truncate the existing one
	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	// update writer and save the previous crc
	w.tail = newTail.File
	w.encoder, err = newFileEncoder(w.tail, prevCrc)
	if err != nil {
		return err
	}
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}
	// atomically move temp wal file to wal file
	if err = w.Sync(); err != nil {
		return err
	}

	var off int64
	off, err = newTail.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "")
	}

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return errors.Wrap(err, "")
	}
	if err = fileutil.Fsync(w.dirFile); err != nil {
		return errors.Wrap(err, "")
	}

	// reopen newTail with its new path so calls to Name() match the wal filename format
	newTail.Close()

	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return errors.Wrap(err, "")
	}
	if _, err = newTail.Seek(off, io.SeekStart); err != nil {
		return errors.Wrap(err, "")
	}

	w.tail = newTail.File
	w.walNames = append(w.walNames, newTail.Name())

	w.encoder, err = newFileEncoder(w.tail, prevCrc)
	if err != nil {
		return errors.Wrap(err, "")
	}

	log.Infof("segmented wal file %v is created", fpath)
	return nil
}

func (w *WAL) Sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	err := fileutil.Fdatasync(w.tail)
	return err
}

func (w *WAL) Close(clean bool) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.readClose != nil {
		if err := w.readClose(); err != nil {
			return err
		}
		w.readClose = nil
	}

	if w.tail != nil {
		if err := w.Sync(); err != nil {
			return err
		}
		if err := w.tail.Close(); err != nil {
			return err
		}
		w.tail = nil
	}

	if err = w.dirFile.Close(); err != nil {
		return
	}

	if clean {
		if err = w.clean(); err != nil {
			return
		}
	}
	return
}

// SaveEntry saves an entry, and always sync the wal
func (w *WAL) SaveEntry(e *walpb.Entry) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err = w.saveEntry(e); err != nil {
		return
	}
	if w.encoder.curOff >= SegmentSizeBytes {
		if err = w.cut(); err != nil {
			return
		}
	} else {
		if err = w.Sync(); err != nil {
			return
		}
	}
	return
}

func (w *WAL) saveEntry(e *walpb.Entry) (err error) {
	// TODO: add MustMarshalTo to reduce one allocation.
	b := pbutil.MustMarshal(e)
	rec := &walpb.Record{Type: entryType, Data: b}
	if err = w.encoder.encode(rec); err != nil {
		return
	}
	w.enti = e.Index
	return
}

func (w *WAL) Save(ents []walpb.Entry) (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// short cut, do not call sync
	if len(ents) == 0 {
		return
	}

	// TODO(xiangli): no more reference operator
	for i := range ents {
		if err = w.saveEntry(&ents[i]); err != nil {
			return
		}
	}

	if w.encoder.curOff >= SegmentSizeBytes {
		if err = w.cut(); err != nil {
			return
		}
	} else {
		if err = w.Sync(); err != nil {
			return
		}
	}
	return
}

func (w *WAL) saveCrc(prevCrc uint32) (err error) {
	err = w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
	return
}

func (w *WAL) seq() uint64 {
	num := len(w.walNames)
	if num == 0 {
		return 0
	}
	seq, _, err := parseWalName(filepath.Base(w.walNames[num-1]))
	if err != nil {
		log.Fatalf("bad wal name %s (%v)", w.walNames[num-1], err)
	}
	return seq
}

func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
