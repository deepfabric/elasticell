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
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/deepfabric/indexer/wal/walpb"
)

func TestNew(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p)
	require.NoError(t, err)
	if g := filepath.Base(w.tail.Name()); g != walName(0, 0) {
		t.Errorf("name = %+v, want %+v", g, walName(0, 0))
	}
	defer w.Close(false)

	// file is preallocated to segment size; only read data written by wal
	off, err := w.tail.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	gd := make([]byte, off)
	f, err := os.Open(filepath.Join(p, filepath.Base(w.tail.Name())))
	require.NoError(t, err)
	defer f.Close()
	_, err = io.ReadFull(f, gd)
	require.NoError(t, err)

	var wb bytes.Buffer
	e := newEncoder(&wb, 0, 0)
	e.flush()
	if !bytes.Equal(gd, wb.Bytes()) {
		t.Errorf("data = %v, want %v", gd, wb.Bytes())
	}
}

func TestNewForInitedDir(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	os.Create(filepath.Join(p, walName(0, 0)))
	if _, err = Create(p); err == nil || err != os.ErrExist {
		t.Errorf("err = %v, want %v", err, os.ErrExist)
	}
}

func TestOpenAtIndex(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	f, err := os.Create(filepath.Join(dir, walName(0, 0)))
	require.NoError(t, err)
	f.Close()

	w, err := Open(dir, walpb.Snapshot{})
	require.NoError(t, err)
	if g := filepath.Base(w.walNames[len(w.walNames)-1]); g != walName(0, 0) {
		require.Equal(t, walName(0, 0), g)
	}
	require.Equal(t, uint64(0), w.seq())
	w.Close(false)

	wname := walName(2, 10)
	f, err = os.Create(filepath.Join(dir, wname))
	require.NoError(t, err)
	f.Close()

	w, err = Open(dir, walpb.Snapshot{Index: 5})
	require.NoError(t, err)
	if g := filepath.Base(w.walNames[len(w.walNames)-1]); g != wname {
		require.Equal(t, wname, g)
	}
	require.Equal(t, uint64(2), w.seq())
	w.Close(false)

	emptydir, err := ioutil.TempDir(os.TempDir(), "waltestempty")
	require.NoError(t, err)
	defer os.RemoveAll(emptydir)
	_, err = Open(emptydir, walpb.Snapshot{})
	require.Equal(t, ErrFileNotFound, errors.Cause(err))
}

// TODO: split it into smaller tests for better readability
func TestCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p)
	require.NoError(t, err)
	defer w.Close(false)

	err = w.cut()
	require.NoError(t, err)
	wname := walName(1, 1)
	if g := filepath.Base(w.tail.Name()); g != wname {
		t.Errorf("name = %s, want %s", g, wname)
	}

	es := []walpb.Entry{{Index: 1, Term: 1, Data: []byte{1}}}
	err = w.Save(es)
	require.NoError(t, err)
	err = w.cut()
	require.NoError(t, err)
	wname = walName(2, 2)
	if g := filepath.Base(w.tail.Name()); g != wname {
		t.Errorf("name = %s, want %s", g, wname)
	}

	// check the state in the last WAL
	// We do check before closing the WAL to ensure that Cut syncs the data
	// into the disk.
	f, err := os.Open(filepath.Join(p, wname))
	require.NoError(t, err)
	defer f.Close()
	nw := &WAL{
		decoder: newDecoder(f),
	}
	_, err = nw.ReadAll()
	require.NoError(t, err)
}

func TestRecover(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p)
	require.NoError(t, err)
	ents := []walpb.Entry{{Index: 1, Term: 1, Data: []byte{1}}, {Index: 2, Term: 2, Data: []byte{2}}}
	err = w.Save(ents)
	require.NoError(t, err)
	w.Close(false)

	w, err = Open(p, walpb.Snapshot{})
	require.NoError(t, err)
	entries, err := w.ReadAll()
	require.NoError(t, err)

	require.Equal(t, ents, entries)
	w.Close(false)
}

func TestSearchIndex(t *testing.T) {
	tests := []struct {
		names []string
		index uint64
		widx  int
		wok   bool
	}{
		{
			[]string{
				"0000000000000000-0000000000000000.wal",
				"0000000000000001-0000000000001000.wal",
				"0000000000000002-0000000000002000.wal",
			},
			0x1000, 1, true,
		},
		{
			[]string{
				"0000000000000001-0000000000004000.wal",
				"0000000000000002-0000000000003000.wal",
				"0000000000000003-0000000000005000.wal",
			},
			0x4000, 1, true,
		},
		{
			[]string{
				"0000000000000001-0000000000002000.wal",
				"0000000000000002-0000000000003000.wal",
				"0000000000000003-0000000000005000.wal",
			},
			0x1000, -1, false,
		},
	}
	for i, tt := range tests {
		idx, ok := searchIndex(tt.names, tt.index)
		if idx != tt.widx {
			t.Errorf("#%d: idx = %d, want %d", i, idx, tt.widx)
		}
		if ok != tt.wok {
			t.Errorf("#%d: ok = %v, want %v", i, ok, tt.wok)
		}
	}
}

func TestScanWalName(t *testing.T) {
	tests := []struct {
		str          string
		wseq, windex uint64
		wok          bool
	}{
		{"0000000000000000-0000000000000000.wal", 0, 0, true},
		{"0000000000000000.wal", 0, 0, false},
		{"0000000000000000-0000000000000000.snap", 0, 0, false},
	}
	for i, tt := range tests {
		s, index, err := parseWalName(tt.str)
		if g := err == nil; g != tt.wok {
			t.Errorf("#%d: ok = %v, want %v", i, g, tt.wok)
		}
		if s != tt.wseq {
			t.Errorf("#%d: seq = %d, want %d", i, s, tt.wseq)
		}
		if index != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, index, tt.windex)
		}
	}
}

func TestRecoverAfterCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	md, err := Create(p)
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		es := []walpb.Entry{{Index: uint64(i)}}
		err = md.Save(es)
		require.NoError(t, err)
		err = md.cut()
		require.NoError(t, err)
	}
	md.Close(false)

	err = os.Remove(filepath.Join(p, walName(4, 4)))
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		w, err := Open(p, walpb.Snapshot{Index: uint64(i)})
		if err != nil {
			if i <= 4 {
				if err != ErrFileNotFound {
					t.Errorf("#%d: err = %v, want %v", i, err, ErrFileNotFound)
				}
			} else {
				t.Errorf("#%d: err = %v, want nil", i, err)
			}
			continue
		}
		entries, err := w.ReadAll()
		if err != nil {
			t.Errorf("#%d: err = %v, want nil", i, err)
			continue
		}
		for j, e := range entries {
			if e.Index != uint64(j+i+1) {
				t.Errorf("#%d: ents[%d].Index = %+v, want %+v", i, j, e.Index, j+i+1)
			}
		}
		w.Close(false)
	}
}

func TestOpenAtUncommittedIndex(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	w, err := Create(p)
	require.NoError(t, err)
	err = w.Save([]walpb.Entry{{Index: 0}})
	require.NoError(t, err)
	w.Close(false)

	w, err = Open(p, walpb.Snapshot{})
	require.NoError(t, err)
	// commit up to index 0, try to read index 1
	_, err = w.ReadAll()
	require.NoError(t, err)
	w.Close(false)
}

// TestOpenForRead tests that OpenForRead can load all files.
// The tests creates WAL directory, and cut out multiple WAL files. Then
// it releases the lock of part of data, and excepts that OpenForRead
// can read out all files even if some are locked for write.
func TestOpenForRead(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)
	// create WAL
	w, err := Create(p)
	require.NoError(t, err)
	defer w.Close(false)
	// make 10 separate files
	for i := 0; i < 10; i++ {
		es := []walpb.Entry{{Index: uint64(i)}}
		err = w.Save(es)
		require.NoError(t, err)
		err = w.cut()
		require.NoError(t, err)
	}

	// All are available for read
	w2, err := OpenForRead(p, walpb.Snapshot{})
	require.NoError(t, err)
	defer w2.Close(false)
	ents, err := w2.ReadAll()
	require.NoError(t, err)
	g := ents[len(ents)-1].Index
	require.Equal(t, uint64(9), g)
}

// TestTailWriteNoSlackSpace ensures that tail writes append if there's no preallocated space.
func TestTailWriteNoSlackSpace(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	// create initial WAL
	w, err := Create(p)
	require.NoError(t, err)
	// write some entries
	for i := 1; i <= 5; i++ {
		es := []walpb.Entry{{Index: uint64(i), Term: 1, Data: []byte{byte(i)}}}
		err = w.Save(es)
		require.NoError(t, err)
	}
	// get rid of slack space by truncating file
	off, serr := w.tail.Seek(0, io.SeekCurrent)
	require.NoError(t, serr)
	terr := w.tail.Truncate(off)
	require.NoError(t, terr)
	w.Close(false)

	// open, write more
	w, err = Open(p, walpb.Snapshot{})
	require.NoError(t, err)
	ents, rerr := w.ReadAll()
	require.NoError(t, rerr)
	require.Equal(t, 5, len(ents))
	// write more entries
	for i := 6; i <= 10; i++ {
		es := []walpb.Entry{{Index: uint64(i), Term: 1, Data: []byte{byte(i)}}}
		err = w.Save(es)
		require.NoError(t, err)
	}
	w.Close(false)

	// confirm all writes
	w, err = Open(p, walpb.Snapshot{})
	require.NoError(t, err)
	ents, rerr = w.ReadAll()
	require.NoError(t, rerr)
	require.Equal(t, 10, len(ents))
	w.Close(false)
}

// TestRestartCreateWal ensures that an interrupted WAL initialization is clobbered on restart
func TestRestartCreateWal(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)

	// make temporary directory so it looks like initialization is interrupted
	tmpdir := filepath.Clean(p) + ".tmp"
	err = os.Mkdir(tmpdir, fileutil.PrivateDirMode)
	require.NoError(t, err)
	_, err = os.OpenFile(filepath.Join(tmpdir, "test"), os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	require.NoError(t, err)

	w, werr := Create(p)
	require.NoError(t, werr)
	w.Close(false)
	if Exist(tmpdir) {
		t.Fatalf("got %q exists, expected it to not exist", tmpdir)
	}

	w, err = OpenForRead(p, walpb.Snapshot{})
	require.NoError(t, err)
	defer w.Close(false)

	_, rerr := w.ReadAll()
	require.NoError(t, rerr)
}

// TestOpenOnTornWrite ensures that entries past the torn write are truncated.
func TestOpenOnTornWrite(t *testing.T) {
	maxEntries := 40
	clobberIdx := 20
	overwriteEntries := 5

	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	require.NoError(t, err)
	defer os.RemoveAll(p)
	w, err := Create(p)
	defer func() {
		if err = w.Close(false); err != nil && err != os.ErrInvalid {
			t.Fatal(err)
		}
	}()
	require.NoError(t, err)

	// get offset of end of each saved entry
	offsets := make([]int64, maxEntries)
	for i := range offsets {
		es := []walpb.Entry{{Index: uint64(i)}}
		err = w.Save(es)
		require.NoError(t, err)
		offsets[i], err = w.tail.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
	}

	fn := filepath.Join(p, filepath.Base(w.tail.Name()))
	w.Close(false)

	// clobber some entry with 0's to simulate a torn write
	f, ferr := os.OpenFile(fn, os.O_WRONLY, fileutil.PrivateFileMode)
	require.NoError(t, ferr)
	defer f.Close()
	_, err = f.Seek(offsets[clobberIdx], io.SeekStart)
	require.NoError(t, err)
	zeros := make([]byte, offsets[clobberIdx+1]-offsets[clobberIdx])
	_, err = f.Write(zeros)
	require.NoError(t, err)
	f.Close()

	w, err = Open(p, walpb.Snapshot{})
	require.NoError(t, err)
	// seek up to clobbered entry
	_, err = w.ReadAll()
	require.NoError(t, err)

	// write a few entries past the clobbered entry
	for i := 0; i < overwriteEntries; i++ {
		// Index is different from old, truncated entries
		es := []walpb.Entry{{Index: uint64(i + clobberIdx), Data: []byte("new")}}
		err = w.Save(es)
		require.NoError(t, err)
	}
	w.Close(false)

	// read back the entries, confirm number of entries matches expectation
	w, err = OpenForRead(p, walpb.Snapshot{})
	require.NoError(t, err)

	ents, rerr := w.ReadAll()
	// CRC error? the old entries were likely never truncated away
	require.NoError(t, rerr)
	wEntries := (clobberIdx - 1) + overwriteEntries
	require.Equal(t, wEntries, len(ents))
}
