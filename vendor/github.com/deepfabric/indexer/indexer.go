package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/davecgh/go-spew/spew"
	"github.com/deepfabric/bkdtree"
	"github.com/deepfabric/indexer/cql"
	"github.com/deepfabric/indexer/wal"
	"github.com/deepfabric/indexer/wal/walpb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// DefaultIndexerMaxOpN is the default value for Indexer.MaxOpN.
	DefaultIndexerMaxOpN = uint64(1000000)
)

var (
	ErrIdxExist    = errors.New("index already exist")
	ErrIdxNotExist = errors.New("index not exist")
)

//Indexer shall be singleton
type Indexer struct {
	MainDir string //the main directory where stores all indices
	// Number of operations performed before performing a snapshot.
	MaxOpN uint64

	rwlock   sync.RWMutex                    //concurrent access of docProts, indices
	docProts map[string]*cql.DocumentWithIdx //index meta, need to persist
	indices  map[string]*Index               //index data, need to persist
	w        *wal.WAL                        //WAL
	opN      uint64
	entIndex uint64
}

//NewIndexer creates an Indexer.
func NewIndexer(mainDir string, overwirte bool, enableWal bool) (ir *Indexer, err error) {
	ir = &Indexer{
		MainDir:  mainDir,
		MaxOpN:   DefaultIndexerMaxOpN,
		entIndex: uint64(0),
	}
	if err = os.MkdirAll(mainDir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if overwirte {
		ir.docProts = make(map[string]*cql.DocumentWithIdx)
		ir.indices = make(map[string]*Index)
		if err = ir.removeIndices(); err != nil {
			return
		}
	} else {
		if err = ir.Open(); err != nil {
			return
		}
	}
	if enableWal {
		walDir := filepath.Join(mainDir, "wal")
		if overwirte {
			if err = os.RemoveAll(walDir); err != nil {
				err = errors.Wrap(err, "")
				return
			}
			if ir.w, err = wal.Create(walDir); err != nil {
				return
			}
		} else {
			if err = ir.replayWal(); err != nil {
				return
			}
		}
	}
	return
}

//Destroy close and remove index files
func (ir *Indexer) Destroy() (err error) {
	ir.rwlock.Lock()
	defer ir.rwlock.Unlock()
	if err = ir.close(); err != nil {
		return
	}
	if err = ir.removeIndices(); err != nil {
		return
	}
	return
}

//Open opens all indices. Assumes ir.MainDir is already populated.
func (ir *Indexer) Open() (err error) {
	ir.rwlock.Lock()
	err = ir.open()
	ir.rwlock.Unlock()
	return
}

//Open opens all indices without holding the lock
func (ir *Indexer) open() (err error) {
	if ir.indices != nil || ir.docProts != nil {
		panic("indexer already open")
	}
	ir.docProts = make(map[string]*cql.DocumentWithIdx)
	ir.indices = make(map[string]*Index)
	if err = ir.readMeta(); err != nil {
		return
	}
	var ind *Index
	for name, docProt := range ir.docProts {
		if ind, err = NewIndexExt(ir.MainDir, docProt.Index); err != nil {
			return
		}
		ir.indices[name] = ind
	}
	if ir.w != nil {
		if err = ir.replayWal(); err != nil {
			return
		}
	}
	return
}

// Close close indexer
func (ir *Indexer) Close() (err error) {
	ir.rwlock.Lock()
	err = ir.close()
	ir.rwlock.Unlock()
	return
}

// Close close indexer without holding the lock
func (ir *Indexer) close() (err error) {
	for _, ind := range ir.indices {
		if err = ind.Close(); err != nil {
			return
		}
	}
	if ir.w != nil {
		if err = ir.w.Close(true); err != nil {
			return
		}
	}
	ir.indices = nil
	ir.docProts = nil
	return
}

// Sync synchronizes index to disk
func (ir *Indexer) Sync() (err error) {
	ir.rwlock.Lock()
	err = ir.sync()
	ir.rwlock.Unlock()
	return
}

// sync synchronizes index to disk without holding the lock
func (ir *Indexer) sync() (err error) {
	for _, ind := range ir.indices {
		if err = ind.Sync(); err != nil {
			return
		}
	}
	if ir.w != nil {
		if err = ir.w.CompactAll(); err != nil {
			return
		}
	}
	return
}

func (ir *Indexer) replayWal() (err error) {
	var w *wal.WAL
	walDir := filepath.Join(ir.MainDir, "wal")
	_, err = os.Stat(walDir)
	if err != nil {
		if !os.IsNotExist(err) {
			err = errors.Wrap(err, "")
			return
		}
		// wal directory doesn't exist
		if w, err = wal.Create(walDir); err != nil {
			return
		}
		ir.w = w
		return
	}
	//replay wal records
	if w, err = wal.OpenAtBeginning(walDir); err != nil {
		return
	}
	var ents []walpb.Entry
	if ents, err = w.ReadAll(); err != nil {
		return
	}
	doc := &cql.DocumentWithIdx{}
	dd := &cql.DocumentDel{}
	for _, ent := range ents {
		switch ent.Type {
		case 0:
			if err = doc.Unmarshal(ent.Data); err != nil {
				err = errors.Wrap(err, "")
				return
			}
			if err = ir.Insert(doc); err != nil {
				return
			}
		default:
			if err = dd.Unmarshal(ent.Data); err != nil {
				err = errors.Wrap(err, "")
				return
			}
			if _, err = ir.Del(dd.Index, dd.DocID); err != nil {
				return
			}
		}
	}
	log.Infof("replayed %v entries in %v", len(ents), walDir)
	ir.w = w
	if err = ir.sync(); err != nil {
		return
	}
	return
}

// GetDocProts dumps docProts
func (ir *Indexer) GetDocProts() (sdump string) {
	ir.rwlock.RLock()
	sdump = spew.Sdump(ir.docProts)
	ir.rwlock.RUnlock()
	return
}

// GetDocProt returns docProt of given index
func (ir *Indexer) GetDocProt(name string) (docProt *cql.DocumentWithIdx) {
	ir.rwlock.RLock()
	docProt, _ = ir.docProts[name]
	ir.rwlock.RUnlock()
	return
}

// CreateIndex creates index
func (ir *Indexer) CreateIndex(docProt *cql.DocumentWithIdx) (err error) {
	ir.rwlock.Lock()
	err = ir.createIndex(docProt)
	ir.rwlock.Unlock()
	return
}

//DestroyIndex destroy given index
func (ir *Indexer) DestroyIndex(name string) (err error) {
	ir.rwlock.Lock()
	delete(ir.indices, name)
	delete(ir.docProts, name)
	err = ir.removeIndex(name)
	ir.rwlock.Unlock()
	return
}

//Insert executes CqlInsert
func (ir *Indexer) Insert(doc *cql.DocumentWithIdx) (err error) {
	var ind *Index
	var found bool
	ir.rwlock.RLock()
	if ind, found = ir.indices[doc.Index]; !found {
		ir.rwlock.RUnlock()
		err = errors.Wrapf(ErrIdxNotExist, "index %v doesn't exist", doc.Index)
		return
	}
	if err = ind.Insert(doc); err != nil {
		ir.rwlock.RUnlock()
		return
	}
	if ir.w != nil {
		var data []byte
		if data, err = doc.Marshal(); err != nil {
			ir.rwlock.RUnlock()
			err = errors.Wrap(err, "")
			return
		}
		entIndex := atomic.AddUint64(&ir.entIndex, uint64(1))
		e := &walpb.Entry{Index: entIndex, Data: data}
		if err = ir.w.SaveEntry(e); err != nil {
			ir.rwlock.RUnlock()
			return
		}
	}
	ir.rwlock.RUnlock()
	if err = ir._IncrementOpN(); err != nil {
		return
	}
	return
}

//Del executes CqlDel. It's allowed that the given index doesn't exist.
func (ir *Indexer) Del(idxName string, docID uint64) (found bool, err error) {
	var ind *Index
	var fnd bool
	ir.rwlock.RLock()
	if ind, fnd = ir.indices[idxName]; !fnd {
		ir.rwlock.RUnlock()
		err = errors.Wrapf(ErrIdxNotExist, "index %v doesn't exist", idxName)
		return
	}
	if found, err = ind.Del(docID); err != nil {
		ir.rwlock.RUnlock()
		return
	}
	if ir.w != nil {
		dd := cql.DocumentDel{
			Index: idxName,
			DocID: docID,
		}
		var data []byte
		if data, err = dd.Marshal(); err != nil {
			ir.rwlock.RUnlock()
			err = errors.Wrap(err, "")
			return
		}
		entIndex := atomic.AddUint64(&ir.entIndex, uint64(1))
		e := &walpb.Entry{Index: entIndex, Type: walpb.EntryType(1), Data: data}
		if err = ir.w.SaveEntry(e); err != nil {
			ir.rwlock.RUnlock()
			return
		}
	}
	ir.rwlock.RUnlock()
	if err = ir._IncrementOpN(); err != nil {
		return
	}
	return
}

// _IncrementOpN increase the operation count by one.
// If the count exceeds the maximum allowed then a snapshot is performed.
func (ir *Indexer) _IncrementOpN() (err error) {
	opN := atomic.AddUint64(&ir.opN, uint64(1))
	if opN <= ir.MaxOpN {
		return
	}
	atomic.StoreUint64(&ir.opN, 0)
	err = ir.Sync()
	return
}

//Select executes CqlSelect.
func (ir *Indexer) Select(q *cql.CqlSelect) (qr *QueryResult, err error) {
	var ind *Index
	var found bool
	ir.rwlock.RLock()
	if ind, found = ir.indices[q.Index]; !found {
		err = errors.Wrap(ErrIdxNotExist, q.Index)
		ir.rwlock.RUnlock()
		return
	}
	ir.rwlock.RUnlock()
	qr, err = ind.Select(q)
	return
}

//Summary returns a summary of all indices.
func (ir *Indexer) Summary() (sum string, err error) {
	var ind *Index
	var name string
	var cnt uint64
	ir.rwlock.RLock()
	defer ir.rwlock.RUnlock()
	for name, ind = range ir.indices {
		if cnt, err = ind.liveDocs.Count(); err != nil {
			return
		}
		sum += fmt.Sprintf("index %s contains %d documents\n", name, cnt)
	}
	return
}

// createIndex creates index without holding the lock
func (ir *Indexer) createIndex(docProt *cql.DocumentWithIdx) (err error) {
	if curDocProt, found := ir.docProts[docProt.Index]; found {
		if isSameSchema(curDocProt, docProt) {
			return
		}
		//TODO: on line schema change
		log.Infof("indexer %v createIndex with the different schema, current one %+v, new one %+v", ir.MainDir, curDocProt, docProt)
		if err = ir.removeIndex(docProt.Index); err != nil {
			return
		}
	}
	if err = indexWriteConf(ir.MainDir, docProt); err != nil {
		return
	}
	var ind *Index
	if ind, err = NewIndex(docProt, ir.MainDir); err != nil {
		return
	}
	ir.indices[docProt.Index] = ind
	ir.docProts[docProt.Index] = docProt
	return
}

//WriteMeta persists Conf and DocProts to files.
func (ir *Indexer) WriteMeta() (err error) {
	ir.rwlock.RLock()
	for _, docProt := range ir.docProts {
		if err = indexWriteConf(ir.MainDir, docProt); err != nil {
			ir.rwlock.RUnlock()
			return
		}
	}
	ir.rwlock.RUnlock()
	return
}

//readMeta parses Conf and DocProts from files.
func (ir *Indexer) readMeta() (err error) {
	var matches [][]string
	patt := `^index_(?P<name>[^.]+)\.json$`
	if matches, err = bkdtree.FilepathGlob(ir.MainDir, patt); err != nil {
		return
	}
	for _, match := range matches {
		var doc cql.DocumentWithIdx
		if err = indexReadConf(ir.MainDir, match[1], &doc); err != nil {
			return
		}
		ir.docProts[match[1]] = &doc
	}
	return
}

func (ir *Indexer) removeIndices() (err error) {
	var matches [][]string
	patt := `^index_(?P<name>[^.]+)\.json$`
	if matches, err = bkdtree.FilepathGlob(ir.MainDir, patt); err != nil {
		return
	}
	for _, match := range matches {
		if err = ir.removeIndex(match[1]); err != nil {
			return
		}
	}
	return
}

func (ir *Indexer) removeIndex(name string) (err error) {
	var fp string
	fp = filepath.Join(ir.MainDir, fmt.Sprintf("index_%s.json", name))
	if err = os.Remove(fp); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	fp = filepath.Join(ir.MainDir, name)
	if err = os.RemoveAll(fp); err != nil {
		err = errors.Wrap(err, "")
	}
	return
}

func (ir *Indexer) GetDocIDFragList() (numList []uint64) {
	ir.rwlock.RLock()
	numList = ir.getDocIDFragList()
	ir.rwlock.RUnlock()
	return
}

func (ir *Indexer) getDocIDFragList() (numList []uint64) {
	numList = []uint64{}
	seen := map[uint64]int{}
	for _, ind := range ir.indices {
		numList2 := ind.GetDocIDFragList()
		for _, num := range numList2 {
			seen[num] = 1
		}
	}
	for num := range seen {
		numList = append(numList, num)
	}
	sort.Slice(numList, func(i, j int) bool { return numList[i] < numList[j] })
	return
}

func (ir *Indexer) CreateSnapshot(snapDir string) (numList []uint64, err error) {
	ir.rwlock.Lock()
	defer ir.rwlock.Unlock()
	if err = ir.sync(); err != nil {
		return
	}
	src := ir.MainDir
	dst := filepath.Join(snapDir, "index")
	if err = os.RemoveAll(dst); err != nil {
		return
	}
	if err = CopyDir(src, dst); err != nil {
		return
	}
	numList = ir.getDocIDFragList()
	return
}

func (ir *Indexer) ApplySnapshot(snapDir string) (err error) {
	ir.rwlock.Lock()
	defer ir.rwlock.Unlock()
	if err = ir.close(); err != nil {
		return
	}
	if err = os.RemoveAll(ir.MainDir); err != nil {
		return
	}
	src := filepath.Join(snapDir, "index")
	dst := ir.MainDir
	_, err = os.Stat(src)
	if os.IsNotExist(err) {
		log.Infof("snapshot source directory %v doesn't exist, treating it as an empty one", src)
		if err = os.MkdirAll(dst, 0700); err != nil {
			err = errors.Wrap(err, "")
			return
		}
	} else {
		if err = CopyDir(src, dst); err != nil {
			return
		}
	}
	if err = ir.open(); err != nil {
		return
	}
	log.Infof("applied snapshot %v, docProts %+v", src, ir.docProts)
	return
}

func isSameSchema(docProt1, docProt2 *cql.DocumentWithIdx) bool {
	if docProt1.Index != docProt2.Index ||
		len(docProt1.Doc.UintProps) != len(docProt2.Doc.UintProps) ||
		len(docProt1.Doc.EnumProps) != len(docProt2.Doc.EnumProps) ||
		len(docProt1.Doc.StrProps) != len(docProt2.Doc.StrProps) {
		return false
	}
	for i := 0; i < len(docProt1.Doc.UintProps); i++ {
		uintProt1 := docProt1.Doc.UintProps[i]
		uintProt2 := docProt2.Doc.UintProps[i]
		if uintProt1.Name != uintProt2.Name ||
			uintProt1.IsFloat != uintProt2.IsFloat ||
			uintProt1.ValLen != uintProt2.ValLen {
			return false
		}
	}
	for i := 0; i < len(docProt1.Doc.EnumProps); i++ {
		enumProt1 := docProt1.Doc.EnumProps[i]
		enumProt2 := docProt2.Doc.EnumProps[i]
		if enumProt1.Name != enumProt2.Name {
			return false
		}
	}
	for i := 0; i < len(docProt1.Doc.StrProps); i++ {
		strProt1 := docProt1.Doc.StrProps[i]
		strProt2 := docProt2.Doc.StrProps[i]
		if strProt1.Name != strProt2.Name {
			return false
		}
	}
	return true
}
