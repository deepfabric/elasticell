package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/deepfabric/bkdtree"
	"github.com/deepfabric/go-datastructures"
	"github.com/deepfabric/indexer/cql"
	"github.com/pilosa/pilosa"
	"github.com/pkg/errors"
)

const (
	LiveDocs string = "__liveDocs" // the directory where stores Index.liveDocs
)

var (
	ErrUnknownProp = errors.New("unknown property")
	ErrDocExist    = errors.New("document already exist")
)

//Index is created by CqlCreate
type Index struct {
	MainDir string
	DocProt *cql.DocumentWithIdx //document prototype. persisted to an index-specific file

	rwlock    sync.RWMutex //concurrent access of frames, liveDocs
	intFrames map[string]*IntFrame
	txtFrames map[string]*TextFrame
	liveDocs  *TextFrame //row 0 of this frame stores a bitmap of live docIDs. other rows are not used.
	dirty     bool
}

// QueryResult is query result
type QueryResult struct {
	Bm *pilosa.Bitmap               // used when no OrderBy given
	Oa *datastructures.OrderedArray // used when OrderBy given
}

// Merge merges other (keep unchagned) into qr
func (qr *QueryResult) Merge(other *QueryResult) {
	qr.Bm.Merge(other.Bm)
	qr.Oa.Merge(other.Oa)
}

// NewQueryResult creates an empty QueryResult
func NewQueryResult(limit int) (qr *QueryResult) {
	qr = &QueryResult{
		Bm: pilosa.NewBitmap(),
		Oa: datastructures.NewOrderedArray(limit),
	}
	return
}

//NewIndex creates index according to given conf, overwrites existing files.
func NewIndex(docProt *cql.DocumentWithIdx, mainDir string) (ind *Index, err error) {
	if err = indexWriteConf(mainDir, docProt); err != nil {
		return
	}
	// ensure per-index sub-directory exists
	indDir := filepath.Join(mainDir, docProt.Index)
	if err = os.MkdirAll(indDir, 0700); err != nil {
		return
	}
	ind = &Index{
		MainDir:   mainDir,
		DocProt:   docProt,
		intFrames: make(map[string]*IntFrame),
		txtFrames: make(map[string]*TextFrame),
	}
	var ifm *IntFrame
	for _, uintProp := range docProt.Doc.UintProps {
		dir := filepath.Join(indDir, uintProp.Name)
		if ifm, err = NewIntFrame(dir, docProt.Index, uintProp.Name, uint(uintProp.ValLen*8), true); err != nil {
			return
		}
		ind.intFrames[uintProp.Name] = ifm
	}
	var tfm *TextFrame
	for _, strProp := range docProt.Doc.StrProps {
		dir := filepath.Join(indDir, strProp.Name)
		if tfm, err = NewTextFrame(dir, docProt.Index, strProp.Name, true); err != nil {
			return
		}
		ind.txtFrames[strProp.Name] = tfm
	}
	dir := filepath.Join(indDir, LiveDocs)
	if tfm, err = NewTextFrame(dir, docProt.Index, LiveDocs, true); err != nil {
		return
	}
	ind.liveDocs = tfm
	return
}

//indexWriteConf persists conf to given path.
func indexWriteConf(mainDir string, docProt *cql.DocumentWithIdx) (err error) {
	if err = os.MkdirAll(mainDir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	fp := filepath.Join(mainDir, fmt.Sprintf("index_%s.json", docProt.Index))
	err = bkdtree.FileMarshal(fp, docProt)
	return
}

//indexReadConf parses conf
func indexReadConf(mainDir string, name string, docProt *cql.DocumentWithIdx) (err error) {
	fp := filepath.Join(mainDir, fmt.Sprintf("index_%s.json", name))
	err = bkdtree.FileUnmarshal(fp, docProt)
	return
}

//Destroy removes data and conf files on disk.
func (ind *Index) Destroy() (err error) {
	ind.rwlock.Lock()
	defer ind.rwlock.Unlock()
	if ind.liveDocs != nil {
		for _, ifm := range ind.intFrames {
			if err = ifm.Destroy(); err != nil {
				return
			}
		}
		for _, tfm := range ind.txtFrames {
			if err = tfm.Destroy(); err != nil {
				return
			}
		}
		if err = ind.liveDocs.Destroy(); err != nil {
			return
		}
		ind.intFrames = nil
		ind.txtFrames = nil
		ind.liveDocs = nil
	}

	paths := make([]string, 0)
	for _, uintProp := range ind.DocProt.Doc.UintProps {
		paths = append(paths, filepath.Join(ind.MainDir, uintProp.Name))
	}
	for _, strProp := range ind.DocProt.Doc.StrProps {
		paths = append(paths, filepath.Join(ind.MainDir, strProp.Name))
	}
	paths = append(paths, filepath.Join(ind.MainDir, LiveDocs))
	paths = append(paths, filepath.Join(ind.MainDir, fmt.Sprintf("index_%s.json", ind.DocProt.Index)))
	for _, fp := range paths {
		if err = os.RemoveAll(fp); err != nil {
			err = errors.Wrap(err, "")
		}
	}
	ind.dirty = false
	return
}

//NewIndexExt create index according to existing files.
func NewIndexExt(mainDir, name string) (ind *Index, err error) {
	docProt := &cql.DocumentWithIdx{}
	if err = indexReadConf(mainDir, name, docProt); err != nil {
		return
	}
	ind = &Index{
		MainDir: mainDir,
		DocProt: docProt,
	}
	err = ind.Open()
	return
}

//Open opens existing index. Assumes MainDir and DocProt is already populated.
func (ind *Index) Open() (err error) {
	ind.rwlock.Lock()
	defer ind.rwlock.Unlock()
	if ind.liveDocs != nil {
		//index is already open
		return
	}
	indDir := filepath.Join(ind.MainDir, ind.DocProt.Index)
	ind.intFrames = make(map[string]*IntFrame)
	var ifm *IntFrame
	for _, uintProp := range ind.DocProt.Doc.UintProps {
		dir := filepath.Join(indDir, uintProp.Name)
		if ifm, err = NewIntFrame(dir, ind.DocProt.Index, uintProp.Name, uint(uintProp.ValLen*8), false); err != nil {
			return
		}
		ind.intFrames[uintProp.Name] = ifm
	}
	ind.txtFrames = make(map[string]*TextFrame)
	var tfm *TextFrame
	for _, strProp := range ind.DocProt.Doc.StrProps {
		dir := filepath.Join(indDir, strProp.Name)
		if tfm, err = NewTextFrame(dir, ind.DocProt.Index, strProp.Name, false); err != nil {
			return
		}
		ind.txtFrames[strProp.Name] = tfm
	}
	dir := filepath.Join(indDir, LiveDocs)
	if tfm, err = NewTextFrame(dir, ind.DocProt.Index, LiveDocs, false); err != nil {
		return
	}
	ind.liveDocs = tfm
	ind.dirty = false
	return
}

//Close closes index
func (ind *Index) Close() (err error) {
	ind.rwlock.Lock()
	defer ind.rwlock.Unlock()
	if ind.liveDocs == nil {
		//index is already closed
		return
	}
	for _, ifm := range ind.intFrames {
		if err = ifm.Close(); err != nil {
			return
		}
	}
	for _, tfm := range ind.txtFrames {
		if err = tfm.Close(); err != nil {
			return
		}
	}
	if err = ind.liveDocs.Close(); err != nil {
		return
	}
	ind.intFrames = nil
	ind.txtFrames = nil
	ind.liveDocs = nil
	ind.dirty = false
	return
}

// Sync synchronizes index to disk
func (ind *Index) Sync() (err error) {
	ind.rwlock.Lock()
	defer ind.rwlock.Unlock()
	if !ind.dirty {
		return
	}
	for _, ifm := range ind.intFrames {
		if err = ifm.Sync(); err != nil {
			return
		}
	}
	for _, tfm := range ind.txtFrames {
		if err = tfm.Sync(); err != nil {
			return
		}
	}
	if err = ind.liveDocs.Sync(); err != nil {
		return
	}
	ind.dirty = false
	return
}

//Insert executes CqlInsert
func (ind *Index) Insert(doc *cql.DocumentWithIdx) (err error) {
	var ifm *IntFrame
	var tfm *TextFrame
	var changed, ok bool
	ind.rwlock.RLock()
	defer ind.rwlock.RUnlock()
	//check if doc.DocID is already there before insertion.
	if changed, err = ind.liveDocs.setBit(0, doc.Doc.DocID); err != nil {
		return
	} else if !changed {
		err = errors.Wrapf(ErrDocExist, "document %v is alaredy there before insertion", doc.Doc.DocID)
		return
	}
	for _, uintProp := range doc.Doc.UintProps {
		if ifm, ok = ind.intFrames[uintProp.Name]; !ok {
			err = errors.Wrapf(ErrUnknownProp, "property %v is missing at index spec, document %v, index spec %v", uintProp.Name, doc, ind.DocProt)
			return
		}
		if err = ifm.DoIndex(doc.Doc.DocID, uintProp.Val); err != nil {
			return
		}
	}
	for _, strProp := range doc.Doc.StrProps {
		if tfm, ok = ind.txtFrames[strProp.Name]; !ok {
			err = errors.Wrapf(ErrUnknownProp, "property %v is missing at index spec, document %v, index spec %v", strProp.Name, doc, ind.DocProt)
			return
		}
		if err = tfm.DoIndex(doc.Doc.DocID, strProp.Val); err != nil {
			return
		}
	}
	ind.dirty = true
	return
}

//Del executes CqlDel. Do mark-deletion only. The caller shall rebuild index in order to recycle disk space.
func (ind *Index) Del(docID uint64) (found bool, err error) {
	var changed bool
	ind.rwlock.RLock()
	defer ind.rwlock.RUnlock()
	if changed, err = ind.liveDocs.clearBit(0, docID); err != nil {
		return
	} else if !changed {
		return
	}
	found = true
	ind.dirty = true
	return
}

//Select executes CqlSelect.
func (ind *Index) Select(q *cql.CqlSelect) (qr *QueryResult, err error) {
	qr = &QueryResult{
		Bm: pilosa.NewBitmap(),
		Oa: datastructures.NewOrderedArray(q.Limit),
	}
	var ifm *IntFrame
	var tfm *TextFrame
	var ok bool
	var prevDocs, docs *pilosa.Bitmap

	ind.rwlock.RLock()
	defer ind.rwlock.RUnlock()
	prevDocs = ind.liveDocs.row(0)
	if prevDocs.Count() == 0 {
		return
	}
	if len(q.StrPreds) != 0 {
		for _, strPred := range q.StrPreds {
			if tfm, ok = ind.txtFrames[strPred.Name]; !ok {
				err = errors.Wrapf(ErrUnknownProp, "property %s not found in index spec", strPred.Name)
				return
			}
			docs = tfm.Query(strPred.ContWord)
			prevDocs = prevDocs.Intersect(docs)
			if prevDocs.Count() == 0 {
				return
			}
		}
	}

	if len(q.UintPreds) == 0 {
		qr.Bm = prevDocs
		return
	}

	var ifmOrder *IntFrame
	for _, uintPred := range q.UintPreds {
		if ifm, ok = ind.intFrames[uintPred.Name]; !ok {
			err = errors.Wrapf(ErrUnknownProp, "property %s not found in index spec", uintPred.Name)
			return
		}
		if q.OrderBy == uintPred.Name {
			ifmOrder = ifm
		}
		var bm *pilosa.Bitmap
		if bm, err = ifm.QueryRangeBetween(uintPred.Low, uintPred.High); err != nil {
			return
		}
		prevDocs = prevDocs.Intersect(bm)
		if prevDocs.Count() == 0 {
			return
		}
	}

	if ifmOrder == nil {
		qr.Bm = prevDocs
	} else {
		var val uint64
		var exists bool
		for _, docID := range prevDocs.Bits() {
			if val, exists, err = ifmOrder.GetValue(docID); err != nil {
				return
			}
			if exists {
				point := bkdtree.Point{
					Vals:     []uint64{val},
					UserData: docID,
				}
				qr.Oa.Put(point)
			}
		}
	}

	return
}

//GetDocIDFragList returns DocID fragment list. Each fragment's size is pilosa.SliceWidth
func (ind *Index) GetDocIDFragList() (numList []uint64) {
	return ind.liveDocs.GetFragList()
}
