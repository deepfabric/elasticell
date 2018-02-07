package indexer

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
)

const (
	MaxUint = ^uint(0)
	MinUint = 0
	MaxInt  = int(MaxUint >> 1)
	MinInt  = -MaxInt - 1
)

// IntFrame represents a string field of an index. Refers to pilosa.Frame and pilosa.View.
type IntFrame struct {
	path      string
	index     string
	name      string
	bitDepth  uint
	rwlock    sync.RWMutex                //concurrent access of fragments
	fragments map[uint64]*pilosa.Fragment //map slice to Fragment
}

// NewIntFrame returns a new instance of frame, and initializes it.
func NewIntFrame(path, index, name string, bitDepth uint, overwrite bool) (f *IntFrame, err error) {
	if overwrite {
		if err = os.RemoveAll(filepath.Join(path, "fragments")); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	f = &IntFrame{
		path:      path,
		index:     index,
		name:      name,
		bitDepth:  bitDepth,
		fragments: make(map[uint64]*pilosa.Fragment),
	}
	err = f.openFragments()
	return
}

//Open opens an existing frame
func (f *IntFrame) Open() (err error) {
	if err = f.openFragments(); err != nil {
		return
	}
	return
}

func (f *IntFrame) openFragments() (err error) {
	var sliceList []uint64
	if sliceList, err = getSliceList(f.path); err != nil {
		return
	}
	for _, slice := range sliceList {
		fp := f.FragmentPath(slice)
		fragment := pilosa.NewFragment(fp, f.index, f.name, pilosa.ViewStandard, slice)
		fragment.MaxOpN = MaxInt
		fragment.CacheType = pilosa.CacheTypeNone
		if err = fragment.Open(); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		f.rwlock.Lock()
		f.fragments[slice] = fragment
		f.rwlock.Unlock()
	}
	return
}

// Close closes all fragments without removing files on disk.
// It's allowed to invoke Close multiple times.
func (f *IntFrame) Close() (err error) {
	if err = f.closeFragments(); err != nil {
		return
	}
	return
}

// Destroy closes all fragments, removes all files on disk.
// It's allowed to invoke Close before or after Destroy.
func (f *IntFrame) Destroy() (err error) {
	if err = f.closeFragments(); err != nil {
		return
	}
	if err = os.RemoveAll(filepath.Join(f.path, "fragments")); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

func (f *IntFrame) closeFragments() (err error) {
	for _, fragment := range f.fragments {
		if err = fragment.Close(); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	f.rwlock.Lock()
	f.fragments = nil
	f.rwlock.Unlock()
	return
}

// Sync synchronizes storage bitmap to disk and reopens it.
func (f *IntFrame) Sync() (err error) {
	f.rwlock.Lock()
	for _, frag := range f.fragments {
		if err = frag.Snapshot(); err != nil {
			f.rwlock.Unlock()
			err = errors.Wrap(err, "")
			return
		}
	}
	f.rwlock.Unlock()
	return
}

// FragmentPath returns the path to a fragment
func (f *IntFrame) FragmentPath(slice uint64) string {
	return filepath.Join(f.path, "fragments", strconv.FormatUint(slice, 10))
}

// Name returns the name the frame was initialized with.
func (f *IntFrame) Name() string { return f.name }

// Index returns the index name the frame was initialized with.
func (f *IntFrame) Index() string { return f.index }

// Path returns the path the frame was initialized with.
func (f *IntFrame) Path() string { return f.path }

// BitDepth returns the bit depth the frame was initialized with.
func (f *IntFrame) BitDepth() uint { return f.bitDepth }

// setValue sets value of a column within the frame, and expands fragments if necessary.
func (f *IntFrame) setValue(colID, val uint64) (changed bool, err error) {
	slice := colID / pilosa.SliceWidth
	f.rwlock.Lock()
	fragment, ok := f.fragments[slice]
	if !ok {
		fp := f.FragmentPath(slice)
		fragment = pilosa.NewFragment(fp, f.index, f.name, pilosa.ViewStandard, slice)
		fragment.MaxOpN = fragment.MaxOpN * 100
		fragment.CacheType = pilosa.CacheTypeNone
		if err = fragment.Open(); err != nil {
			err = errors.Wrap(err, "")
			f.rwlock.Unlock()
			return
		}
		f.fragments[slice] = fragment
	}
	f.rwlock.Unlock()
	changed, err = fragment.SetFieldValue(colID, f.bitDepth, val)
	return
}

// GetValue returns value of a column within the frame.
func (f *IntFrame) GetValue(docID uint64) (val uint64, exists bool, err error) {
	slice := docID / pilosa.SliceWidth
	f.rwlock.RLock()
	fragment, ok := f.fragments[slice]
	f.rwlock.RUnlock()
	if !ok {
		return
	}
	val, exists, err = fragment.FieldValue(docID, f.bitDepth)
	return
}

// DoIndex parses and index a field.
func (f *IntFrame) DoIndex(docID uint64, val uint64) (err error) {
	_, err = f.setValue(docID, val)
	return
}

//QueryRange query which documents' value is inside the given range.
func (f *IntFrame) QueryRange(op pql.Token, predicate uint64) (bm *pilosa.Bitmap, err error) {
	var bm2 *pilosa.Bitmap
	bm = pilosa.NewBitmap()
	for _, frag := range f.fragments {
		bm2, err = frag.FieldRange(op, f.bitDepth, predicate)
		if err != nil {
			return
		}
		bm = bm.Union(bm2)
	}
	return
}

//QueryRangeBetween query which documents' value is inside the given range.
func (f *IntFrame) QueryRangeBetween(predicateMin, predicateMax uint64) (bm *pilosa.Bitmap, err error) {
	var bm2 *pilosa.Bitmap
	bm = pilosa.NewBitmap()
	for _, frag := range f.fragments {
		bm2, err = frag.FieldRangeBetween(f.bitDepth, predicateMin, predicateMax)
		if err != nil {
			return
		}
		bm = bm.Union(bm2)
	}
	return
}
