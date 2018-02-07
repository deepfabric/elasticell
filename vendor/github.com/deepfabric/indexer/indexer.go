package indexer

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/deepfabric/bkdtree"
	"github.com/deepfabric/indexer/cql"
	"github.com/pkg/errors"
)

//Indexer shall be singleton
type Indexer struct {
	MainDir string //the main directory where stores all indices

	rwlock   sync.RWMutex                    //concurrent access of docProts, indices
	docProts map[string]*cql.DocumentWithIdx //index meta, need to persist
	indices  map[string]*Index               //index data, need to persist
}

type ErrIdxNotExist struct {
	idxName string
}

func (e *ErrIdxNotExist) Error() string {
	return fmt.Sprintf("index %s doesn't exist", e.idxName)
}

type ErrIdxExist struct {
	idxName string
}

func (e *ErrIdxExist) Error() string {
	return fmt.Sprintf("index %s already exist", e.idxName)
}

//NewIndexer creates an Indexer.
func NewIndexer(mainDir string, overwirte bool) (ir *Indexer, err error) {
	ir = &Indexer{
		MainDir: mainDir,
	}
	if err = os.MkdirAll(mainDir, 0700); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	if overwirte {
		ir.docProts = make(map[string]*cql.DocumentWithIdx)
		ir.indices = make(map[string]*Index)
		err = ir.removeIndices()
	} else {
		err = ir.Open()
	}
	return
}

//Destroy close and remove index files
func (ir *Indexer) Destroy() (err error) {
	if err = ir.Close(); err != nil {
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
	defer ir.rwlock.Unlock()
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
	return
}

// Close close indexer
func (ir *Indexer) Close() (err error) {
	ir.rwlock.Lock()
	defer ir.rwlock.Unlock()
	for _, ind := range ir.indices {
		if err = ind.Close(); err != nil {
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
	defer ir.rwlock.Unlock()
	for _, ind := range ir.indices {
		if err = ind.Sync(); err != nil {
			return
		}
	}
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
	defer ir.rwlock.Unlock()
	delete(ir.indices, name)
	delete(ir.docProts, name)
	err = ir.removeIndex(name)
	return
}

//Insert executes CqlInsert. If the given index doesn't exist, create it before insertion.
func (ir *Indexer) Insert(doc *cql.DocumentWithIdx) (err error) {
	var ind *Index
	var found bool
	ir.rwlock.RLock()
	if ind, found = ir.indices[doc.Index]; !found {
		if err = ir.createIndex(doc); err != nil {
			ir.rwlock.RUnlock()
			return
		}
		ind, found = ir.indices[doc.Index]
	}
	ir.rwlock.RUnlock()
	err = ind.Insert(doc)
	return
}

//Del executes CqlDel. It's allowed that the given index doesn't exist.
func (ir *Indexer) Del(idxName string, docID uint64) (found bool, err error) {
	var ind *Index
	var fnd bool
	ir.rwlock.RLock()
	if ind, fnd = ir.indices[idxName]; !fnd {
		ir.rwlock.RUnlock()
		return
	}
	ir.rwlock.RUnlock()
	found, err = ind.Del(docID)
	return
}

//Select executes CqlSelect.
func (ir *Indexer) Select(q *cql.CqlSelect) (qr *QueryResult, err error) {
	var ind *Index
	var found bool
	ir.rwlock.RLock()
	if ind, found = ir.indices[q.Index]; !found {
		err = &ErrIdxNotExist{idxName: q.Index}
		err = errors.Wrap(err, "")
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
	if _, found := ir.docProts[docProt.Index]; found {
		err = &ErrIdxExist{idxName: docProt.Index}
		err = errors.Wrap(err, "")
		return
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

//writeMeta persists Conf and DocProts to files.
func (ir *Indexer) writeMeta() (err error) {
	for _, docProt := range ir.docProts {
		if err = indexWriteConf(ir.MainDir, docProt); err != nil {
			return
		}
	}
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
