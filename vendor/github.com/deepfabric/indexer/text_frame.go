package indexer

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/deepfabric/bkdtree"
	"github.com/pilosa/pilosa"
	"github.com/pkg/errors"
)

// TextFrame represents a string field of an index. Refers to pilosa.Frame and pilosa.View.
type TextFrame struct {
	path  string
	index string
	name  string

	rwlock    sync.RWMutex                //concurrent access of fragments
	fragments map[uint64]*pilosa.Fragment //map slice to Fragment
	td        *TermDict
}

// NewTextFrame returns a new instance of frame, and initializes it.
func NewTextFrame(path, index, name string, overwrite bool) (f *TextFrame, err error) {
	var td *TermDict
	if td, err = NewTermDict(path, overwrite); err != nil {
		return
	}
	if overwrite {
		if err = os.RemoveAll(filepath.Join(path, "fragments")); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	}
	f = &TextFrame{
		path:      path,
		index:     index,
		name:      name,
		td:        td,
		fragments: make(map[uint64]*pilosa.Fragment),
	}
	err = f.openFragments()
	return
}

//Open opens an existing frame
func (f *TextFrame) Open() (err error) {
	if err = f.openFragments(); err != nil {
		return
	}
	err = f.td.Open()
	return
}

func (f *TextFrame) openFragments() (err error) {
	var sliceList []uint64
	if sliceList, err = getSliceList(f.path); err != nil {
		return
	}
	for _, slice := range sliceList {
		fp := f.FragmentPath(slice)
		fragment := pilosa.NewFragment(fp, f.index, f.name, pilosa.ViewStandard, slice)
		fragment.MaxOpN = fragment.MaxOpN * 100
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

func getSliceList(dir string) (numList []uint64, err error) {
	var num uint64
	var matches [][]string
	fragDir := filepath.Join(dir, "fragments")
	if err = os.MkdirAll(fragDir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if matches, err = bkdtree.FilepathGlob(fragDir, "^(?P<num>[0-9]+)$"); err != nil {
		return
	}
	for _, match := range matches {
		num, err = strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			err = errors.Wrap(err, "")
			return
		}
		numList = append(numList, num)
	}
	return
}

// Close closes all fragments without removing files on disk.
// It's allowed to invoke Close multiple times.
func (f *TextFrame) Close() (err error) {
	if err = f.closeFragments(); err != nil {
		return
	}
	err = f.td.Close()
	return
}

// Destroy closes all fragments, removes all files on disk.
// It's allowed to invoke Close before or after Destroy.
func (f *TextFrame) Destroy() (err error) {
	if err = f.closeFragments(); err != nil {
		return
	}
	if err = os.RemoveAll(filepath.Join(f.path, "fragments")); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	err = f.td.Destroy()
	return
}

func (f *TextFrame) closeFragments() (err error) {
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
func (f *TextFrame) Sync() (err error) {
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
func (f *TextFrame) FragmentPath(slice uint64) string {
	return filepath.Join(f.path, "fragments", strconv.FormatUint(slice, 10))
}

// Name returns the name the frame was initialized with.
func (f *TextFrame) Name() string { return f.name }

// Index returns the index name the frame was initialized with.
func (f *TextFrame) Index() string { return f.index }

// Path returns the path the frame was initialized with.
func (f *TextFrame) Path() string { return f.path }

// setBit sets a bit within the frame, and expands fragments if necessary.
func (f *TextFrame) setBit(rowID, colID uint64) (changed bool, err error) {
	slice := colID / pilosa.SliceWidth
	f.rwlock.Lock()
	fragment, ok := f.fragments[slice]
	if !ok {
		fp := f.FragmentPath(slice)
		fragment = pilosa.NewFragment(fp, f.index, f.name, pilosa.ViewStandard, slice)
		fragment.MaxOpN = MaxInt
		fragment.CacheType = pilosa.CacheTypeNone
		if err = fragment.Open(); err != nil {
			err = errors.Wrap(err, "")
			f.rwlock.Unlock()
			return
		}
		f.fragments[slice] = fragment
	}
	f.rwlock.Unlock()
	changed, err = fragment.SetBit(rowID, colID)
	return
}

// clearBit clears a bit within the frame.
func (f *TextFrame) clearBit(rowID, colID uint64) (changed bool, err error) {
	slice := colID / pilosa.SliceWidth
	f.rwlock.RLock()
	fragment, ok := f.fragments[slice]
	f.rwlock.RUnlock()
	if !ok {
		return
	}
	changed, err = fragment.ClearBit(rowID, colID)
	return
}

//row returns the given row as a pilosa.Bitmap.
func (f *TextFrame) row(rowID uint64) (bm *pilosa.Bitmap) {
	bm = pilosa.NewBitmap()
	f.rwlock.RLock()
	for _, fragment := range f.fragments {
		bm2 := fragment.Row(rowID)
		bm.Merge(bm2)
	}
	f.rwlock.RUnlock()
	return
}

// Bits returns bits set in frame.
func (f *TextFrame) Bits() (bits map[uint64][]uint64, err error) {
	var ok bool
	bits = make(map[uint64][]uint64)
	var columns []uint64
	f.rwlock.RLock()
	defer f.rwlock.RUnlock()
	for _, fragment := range f.fragments {
		err = fragment.ForEachBit(
			func(rowID, columnID uint64) error {
				columns, ok = bits[rowID]
				if ok {
					columns = append(columns, columnID)
				} else {
					columns = []uint64{columnID}
				}
				bits[rowID] = columns
				return nil
			},
		)
		if err != nil {
			return
		}
	}
	return
}

// Count returns number of bits set in frame.
func (f *TextFrame) Count() (cnt uint64, err error) {
	f.rwlock.RLock()
	defer f.rwlock.RUnlock()
	for _, fragment := range f.fragments {
		err = fragment.ForEachBit(
			func(rowID, columnID uint64) error {
				cnt++
				return nil
			},
		)
		if err != nil {
			return
		}
	}
	return
}

// DoIndex parses and index a field.
func (f *TextFrame) DoIndex(docID uint64, text string) (err error) {
	//https://stackoverflow.com/questions/13737745/split-a-string-on-whitespace-in-go
	/*terms := strings.Fields(text)
	for i, term := range terms {
		terms[i] = strings.ToLower(term)
	}*/
	terms := ParseWords(text)
	ids, err := f.td.CreateTermsIfNotExist(terms)
	if err != nil {
		return
	}
	for _, termID := range ids {
		if _, err = f.setBit(termID, docID); err != nil {
			return
		}
	}
	return
}

//Query query which documents contain the given term.
func (f *TextFrame) Query(text string) (bm *pilosa.Bitmap) {
	words := ParseWords(text)
	var bm2 *pilosa.Bitmap
	for _, word := range words {
		termID, found := f.td.GetTermID(word)
		if !found {
			bm = pilosa.NewBitmap()
			return
		}
		bm2 = f.row(termID)
		if bm != nil {
			bm = bm.Intersect(bm2)
		} else {
			bm = bm2
		}
	}
	return
}

var asciiSpace = [128]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

//ParseWords parses text(encoded in UTF-8) for words.
//A word is a non-ascii-space lowered ASCII character sequence, or a non-ASCII non-unicode-space non-chinese-punctuate character.
//Note: words are not de-duplicated.
func ParseWords(text string) (words []string) {
	lenText := len(text)
	words = make([]string, 0, lenText/3)
	i := 0
	for i < lenText {
		j := i
		var c byte
		for j < lenText {
			if c = text[j]; c < 0x80 && asciiSpace[c] == 0 && unicode.IsPrint(rune(c)) && !unicode.IsPunct(rune(c)) {
				j++
			} else {
				break
			}
		}
		if i < j {
			// text[i:j] is a printable non-space ASCII character sequence.
			words = append(words, strings.ToLower(text[i:j]))
			i = j
		} else if c < 0x80 {
			// i==j, text[i] is an ascii space, non-printable or punctuation character.
			i++
		} else {
			// i==j, text[i] is the begin of an non-ascii character
			r, w := utf8.DecodeRuneInString(text[i:])
			if unicode.IsPrint(rune(c)) && !unicode.IsSpace(r) && !unicode.IsPunct(r) {
				words = append(words, text[i:i+w])
			}
			i += w
		}
	}
	return
}
