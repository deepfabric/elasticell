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
	"encoding/binary"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/indexer"
	"github.com/deepfabric/indexer/cql"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func (s *Store) getCell(cellID uint64) (cell *metapb.Cell) {
	pr := s.replicatesMap.get(cellID)
	if pr == nil {
		return
	}
	c := pr.getStore().getCell()
	cell = &c
	return
}

func (s *Store) loadIndices() (err error) {
	indicesFp := filepath.Join(globalCfg.DataPath, "index", "indices.json")
	if err = util.FileUnmarshal(indicesFp, &s.indices); err != nil {
		log.Errorf("store-index[%d]: failed to load indices definition\n%+v", s.GetID(), err)
	}
	reExps := make(map[string]*regexp.Regexp)
	for _, idxDef := range s.indices {
		reExps[idxDef.GetName()] = regexp.MustCompile(idxDef.GetKeyPattern())
	}
	s.reExps = reExps
	docProts := make(map[string]*cql.Document)
	var docProt *cql.DocumentWithIdx
	for _, idxDef := range s.indices {
		if docProt, err = convertToDocProt(idxDef); err != nil {
			return
		}
		docProts[idxDef.GetName()] = &docProt.Document
	}
	s.docProts = docProts
	return
}

func (s *Store) persistIndices() (err error) {
	indicesFp := filepath.Join(globalCfg.DataPath, "index", "indices.json")
	if err = util.FileMarshal(indicesFp, s.indices); err != nil {
		log.Errorf("store-index[%d]: failed to persist indices definition\n%+v", s.GetID(), err)
	}
	return
}

func (s *Store) allocateDocID(cellID uint64) (docID uint64, err error) {
	var idxerExt *IndexerExt
	var docIDB []byte
	if idxerExt, err = s.getIndexer(cellID); err != nil {
		return
	}
	if idxerExt.NextDocID == 0 {
		if docIDB, err = s.getKVEngine().Get(getCellNextDocIDKey(cellID)); err != nil {
			return
		}
		if docIDB != nil && len(docIDB) != 0 {
			idxerExt.NextDocID = binary.BigEndian.Uint64(docIDB)
		}
	}
	if idxerExt.NextDocID&0xffff == 0 {
		var rsp *pdpb.AllocIDRsp
		rsp, err = s.pdClient.AllocID(context.TODO(), new(pdpb.AllocIDReq))
		if err != nil {
			return
		}
		idxerExt.NextDocID = rsp.GetID() << 16
	}
	docID = idxerExt.NextDocID
	idxerExt.NextDocID++
	if docIDB == nil || len(docIDB) <= 8 {
		docIDB = make([]byte, 8)
	}
	binary.BigEndian.PutUint64(docIDB, idxerExt.NextDocID)
	err = s.getKVEngine().Set(getCellNextDocIDKey(cellID), docIDB)
	return
}

func (s *Store) getIndexer(cellID uint64) (idxerExt *IndexerExt, err error) {
	var idxer *indexer.Indexer
	var ok bool
	var docProt *cql.DocumentWithIdx
	if idxerExt, ok = s.indexers[cellID]; !ok {
		idxerExt = &IndexerExt{}
	} else if idxerExt.Indexer != nil {
		// already initialized
		return
	} else {
		err = errors.Errorf("indexer %d is not initialized correctly", cellID)
		return
	}
	indicesDir := filepath.Join(globalCfg.DataPath, "index", fmt.Sprintf("%d", cellID))
	idxer, err = indexer.NewIndexer(indicesDir, false)
	for _, idxDef := range s.indices {
		//creation shall be idempotent
		if docProt, err = convertToDocProt(idxDef); err != nil {
			return
		}
		if err = idxer.CreateIndex(docProt); err != nil {
			return
		}
	}
	idxerExt.Indexer = idxer
	s.indexers[cellID] = idxerExt
	return
}

func (s *Store) handleIndicesChange(rspIndices []*pdpb.IndexDef) (err error) {
	indicesNew := make(map[string]*pdpb.IndexDef)
	for _, idxDefNew := range rspIndices {
		indicesNew[idxDefNew.GetName()] = idxDefNew
	}
	s.rwlock.RLock()
	if reflect.DeepEqual(s.indices, indicesNew) {
		s.rwlock.RUnlock()
		return
	}
	s.rwlock.RUnlock()
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	reExpsNew := make(map[string]*regexp.Regexp)
	for _, idxDefNew := range rspIndices {
		reExpsNew[idxDefNew.GetName()] = regexp.MustCompile(idxDefNew.GetKeyPattern())
	}
	s.reExps = reExpsNew
	delta := diffIndices(s.indices, indicesNew)
	for _, idxDef := range delta.toDelete {
		//deletion shall be idempotent
		for _, idxerExt := range s.indexers {
			if err = idxerExt.Indexer.DestroyIndex(idxDef.GetName()); err != nil {
				return
			}
		}
		delete(s.docProts, idxDef.GetName())
		log.Infof("store-index[%d]: deleted index %+v", s.GetID(), idxDef)
	}
	var docProt *cql.DocumentWithIdx
	for _, idxDef := range delta.toCreate {
		//creation shall be idempotent
		if docProt, err = convertToDocProt(idxDef); err != nil {
			return
		}
		for _, idxerExt := range s.indexers {
			if err = idxerExt.Indexer.CreateIndex(docProt); err != nil {
				return
			}
		}
		s.docProts[idxDef.GetName()] = &docProt.Document
		log.Infof("store-index[%d]: created index %+v", s.GetID(), idxDef)
	}
	if len(delta.toDelete) != 0 || len(delta.toCreate) != 0 {
		s.indices = indicesNew
		if err = s.persistIndices(); err != nil {
			return
		}
		log.Infof("store-index[%d]: persisted index definion %+v", s.GetID(), indicesNew)
	}
	return
}

func (s *Store) readyToServeIndex(ctx context.Context) {
	listEng := s.getListEngine()
	idxReqQueueKey := getIdxReqQueueKey()
	tickChan := time.Tick(5000 * time.Millisecond)
	wb := s.engine.GetKVEngine().NewWriteBatch()
	var idxKeyReq *pdpb.IndexKeyRequest
	var idxSplitReq *pdpb.IndexSplitRequest
	var idxDestroyReq *pdpb.IndexDestroyCellRequest
	var idxRebuildReq *pdpb.IndexRebuildCellRequest
	var idxReqB []byte
	var numProcessed int
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Infof("store-index[%d]: readyToServeIndex stopped", s.GetID())
			return
		case <-tickChan:
			for {
				idxReqB, err = listEng.LPop(idxReqQueueKey)
				if err != nil {
					log.Errorf("store-index[%d]: failed to LPop idxReqQueueKey\n%+v", s.GetID(), err)
					continue
				} else if idxReqB == nil || len(idxReqB) == 0 {
					// queue is empty
					break
				}
				idxReq := &pdpb.IndexRequest{}
				if err = idxReq.Unmarshal(idxReqB); err != nil {
					log.Errorf("store-index[%d]: failed to decode IndexRequest\n%+v", s.GetID(), err)
					continue
				}
				log.Debugf("store-index[%d]: got idxReq %+v", s.GetID(), idxReq)
				if idxKeyReq = idxReq.GetIdxKey(); idxKeyReq != nil {
					for _, key := range idxKeyReq.GetDataKeys() {
						err = s.deleteIndexedKey(idxKeyReq.CellID, idxKeyReq.GetIdxName(), key, wb)
						if idxKeyReq.IsDel {
							if err != nil {
								log.Errorf("store-index[%d.%d]: failed to delete indexed key %+v from index %s\n%+v",
									s.GetID(), idxKeyReq.CellID, key, idxKeyReq.GetIdxName(), err)
								continue
							}
							log.Debugf("store-index[%d.%d]: deleted key %+v from index %s",
								s.GetID(), idxKeyReq.CellID, key, idxKeyReq.GetIdxName())
						} else {
							if err = s.addIndexedKey(idxKeyReq.CellID, idxKeyReq.GetIdxName(), 0, key, wb); err != nil {
								log.Errorf("store-index[%d]: failed to add key %s to index %s\n%+v", s.GetID(), key, idxKeyReq.GetIdxName(), err)
								continue
							}
						}
					}
					numProcessed++
				}
				if idxSplitReq = idxReq.GetIdxSplit(); idxSplitReq != nil {
					if err = s.indexSplitCell(idxSplitReq, wb); err != nil {
						log.Errorf("store-index[%d]: failed to handle split %+v\n%+v", s.GetID(), idxSplitReq, err)
					}
					numProcessed++
				}
				if idxDestroyReq = idxReq.GetIdxDestroy(); idxDestroyReq != nil {
					if err = s.indexDestroyCell(idxDestroyReq, wb); err != nil {
						log.Errorf("store-index[%d]: failed to handle destroy %+v\n%+v", s.GetID(), idxDestroyReq, err)
					}
					numProcessed++
				}
				if idxRebuildReq = idxReq.GetIdxRebuild(); idxRebuildReq != nil {
					if err = s.indexRebuildCell(idxRebuildReq, wb); err != nil {
						log.Errorf("store-index[%d]: failed to handle rebuild %+v\n%+v", s.GetID(), idxRebuildReq, err)
					}
					numProcessed++
				}
			}
			if numProcessed != 0 {
				if err = s.engine.GetKVEngine().Write(wb); err != nil {
					log.Errorf("store-index[%d]: failed to write batch index\n%+v", s.GetID(), err)
				}
				log.Debugf("store-index[%d]: executed write batch %+v", s.GetID(), wb)
				wb = s.engine.GetKVEngine().NewWriteBatch()
				numProcessed = 0
			}
		}
	}
}

func (s *Store) deleteIndexedKey(cellID uint64, idxNameIn string, dataKey []byte, wb storage.WriteBatch) (err error) {
	var idxerExt *IndexerExt
	var metaValB []byte
	if metaValB, err = s.engine.GetDataEngine().GetIndexInfo(dataKey); err != nil || len(metaValB) == 0 {
		return
	}
	metaVal := &pdpb.KeyMetaVal{}
	if err = metaVal.Unmarshal(metaValB); err != nil {
		return
	}
	idxName, docID := metaVal.GetIdxName(), metaVal.GetDocID()
	if idxName != idxNameIn {
		//TODO(yzc): understand how this happen. Could be inserting data during changing an index' keyPattern?
		//TODO(yzc): add metric?
	}

	if idxerExt, err = s.getIndexer(cellID); err != nil {
		return
	}
	if _, err = idxerExt.Indexer.Del(idxName, docID); err != nil {
		return
	}
	if err = wb.Delete(getDocIDKey(docID)); err != nil {
		return
	}
	// Needn't to clear indexInfo. It is harmless.
	return
}

func (s *Store) addIndexedKey(cellID uint64, idxNameIn string, docID uint64, dataKey []byte, wb storage.WriteBatch) (err error) {
	var idxerExt *IndexerExt
	var metaVal *pdpb.KeyMetaVal
	var metaValB []byte
	var doc *cql.DocumentWithIdx
	var ok bool

	if docID == 0 {
		// allocate docID
		if docID, err = s.allocateDocID(cellID); err != nil {
			return
		}
		metaVal = &pdpb.KeyMetaVal{
			IdxName: idxNameIn,
			DocID:   docID,
		}
		if metaValB, err = metaVal.Marshal(); err != nil {
			return
		}
		if err = s.engine.GetDataEngine().SetIndexInfo(dataKey, metaValB); err != nil {
			return
		}
		if err = wb.Set(getDocIDKey(docID), dataKey); err != nil {
			return
		}
	}

	var idxDef *pdpb.IndexDef
	var pairs []*raftcmdpb.FVPair
	hashEng := s.engine.GetHashEngine()
	if pairs, err = hashEng.HGetAll(dataKey); err != nil {
		return
	}
	if idxDef, ok = s.indices[idxNameIn]; !ok {
		err = errors.Errorf("index %s doesn't exist", idxNameIn)
		return
	}
	if idxerExt, err = s.getIndexer(cellID); err != nil {
		return
	}

	if doc, err = convertToDocument(idxDef, docID, pairs); err != nil {
		return
	}
	if err = idxerExt.Indexer.Insert(doc); err != nil {
		return
	}
	log.Debugf("store-index[%d.%d]: added dataKey %+v to index %s, docID %d, paris %+v",
		s.GetID(), cellID, dataKey, idxNameIn, docID, pairs)
	return
}

func (s *Store) indexSplitCell(idxSplitReq *pdpb.IndexSplitRequest, wb storage.WriteBatch) (err error) {
	var cellL, cellR *metapb.Cell
	if cellL = s.getCell(idxSplitReq.LeftCellID); cellL == nil {
		log.Infof("store-index[%d]: ignored %+v due to left cell %d is gone.", s.GetID(), idxSplitReq.LeftCellID)
		return
	}
	if cellR = s.getCell(idxSplitReq.RightCellID); cellR == nil {
		log.Infof("store-index[%d]: ignored %+v due to right cell %d is gone.", s.GetID(), idxSplitReq.RightCellID)
		return
	}
	//cellR could has been splitted after idxSplitReq creation.
	//Use the up-to-date range to keep scan range as small as possible.
	start := encStartKey(cellR)
	end := encEndKey(cellR)

	var idxerExtL *IndexerExt
	if idxerExtL, err = s.getIndexer(idxSplitReq.LeftCellID); err != nil {
		return
	}
	if _, err = s.getIndexer(idxSplitReq.RightCellID); err != nil {
		return
	}

	var scanned, indexed int
	err = s.engine.GetDataEngine().ScanIndexInfo(start, end, true, func(dataKey, metaValB []byte) (err error) {
		scanned++
		if metaValB == nil || len(metaValB) == 0 {
			return
		}
		metaVal := &pdpb.KeyMetaVal{}
		if err = metaVal.Unmarshal(metaValB); err != nil {
			return
		}
		idxName, docID := metaVal.GetIdxName(), metaVal.GetDocID()
		if idxName != "" {
			if _, err = idxerExtL.Indexer.Del(idxName, docID); err != nil {
				return
			}

			if err = s.addIndexedKey(idxSplitReq.RightCellID, idxName, docID, dataKey, wb); err != nil {
				return
			}
			indexed++
		}
		return
	})
	log.Infof("store-index[%d]: done cell split %+v, has scanned %d dataKeys, has indexed %d dataKeys.", s.GetID(), idxSplitReq, scanned, indexed)
	return
}

func (s *Store) indexDestroyCell(idxDestroyReq *pdpb.IndexDestroyCellRequest, wb storage.WriteBatch) (err error) {
	var cell *metapb.Cell
	if cell = s.getCell(idxDestroyReq.CellID); cell == nil {
		log.Infof("store-index[%d]: ignored %+v due to cell %d is gone.", s.GetID(), idxDestroyReq.CellID)
		return
	}
	start := encStartKey(cell)
	end := encEndKey(cell)

	var idxerExt *IndexerExt
	cellID := idxDestroyReq.GetCellID()
	if idxerExt, err = s.getIndexer(cellID); err != nil {
		return
	}
	delete(s.indexers, idxDestroyReq.GetCellID())
	if err = idxerExt.Indexer.Destroy(); err != nil {
		return
	}
	if err = wb.Delete(getCellNextDocIDKey(cellID)); err != nil {
		return
	}
	var scanned, indexed int
	err = s.engine.GetDataEngine().ScanIndexInfo(start, end, true, func(dataKey, metaValB []byte) (err error) {
		scanned++
		if metaValB == nil || len(metaValB) == 0 {
			return
		}
		metaVal := &pdpb.KeyMetaVal{}
		if err = metaVal.Unmarshal(metaValB); err != nil {
			return
		}
		docID := metaVal.GetDocID()
		if err = wb.Delete(getDocIDKey(docID)); err != nil {
			return
		}
		// Let garbage IndexInfo be there since it's harmless.
		indexed++
		return
	})
	log.Infof("store-index[%d]: done cell destroy %+v, has scanned %d dataKeys, has indexed %d dataKeys.", s.GetID(), idxDestroyReq, scanned, indexed)
	return
}

func (s *Store) indexRebuildCell(idxRebuildReq *pdpb.IndexRebuildCellRequest, wb storage.WriteBatch) (err error) {
	var cell *metapb.Cell
	if cell = s.getCell(idxRebuildReq.CellID); cell == nil {
		log.Infof("store-index[%d]: ignored %+v due to cell %d is gone.", s.GetID(), idxRebuildReq.CellID)
		return
	}
	start := encStartKey(cell)
	end := encEndKey(cell)

	var idxerExt *IndexerExt

	cellID := idxRebuildReq.GetCellID()
	if idxerExt, err = s.getIndexer(cellID); err != nil {
		return
	}
	if err = idxerExt.Indexer.Destroy(); err != nil {
		return
	}
	if err = idxerExt.Indexer.Open(); err != nil {
		return
	}

	var scanned, indexed int
	err = s.engine.GetDataEngine().ScanIndexInfo(start, end, false, func(dataKey, metaValB []byte) (err error) {
		scanned++
		if metaValB != nil || len(metaValB) != 0 {
			metaVal := &pdpb.KeyMetaVal{}
			if err = metaVal.Unmarshal(metaValB); err != nil {
				return
			}
			docID := metaVal.GetDocID()
			if err = wb.Delete(getDocIDKey(docID)); err != nil {
				return
			}
		}
		var idxName string
		for name, reExp := range s.reExps {
			matched := reExp.Match(getOriginKey(dataKey))
			if matched {
				idxName = name
				break
			}
		}
		if idxName != "" {
			if err = s.addIndexedKey(cellID, idxName, 0, dataKey, wb); err != nil {
				return
			}
			indexed++
		}
		return
	})
	log.Infof("store-index[%d]: done cell index rebuild %+v, has scanned %d dataKeys, has indexed %d dataKeys", s.GetID(), idxRebuildReq, scanned, indexed)
	return
}

func convertToDocProt(idxDef *pdpb.IndexDef) (docProt *cql.DocumentWithIdx, err error) {
	uintProps := make([]cql.UintProp, 0)
	strProps := make([]cql.StrProp, 0)
	for _, f := range idxDef.Fields {
		switch f.GetType() {
		case pdpb.Text:
			strProps = append(strProps, cql.StrProp{Name: f.GetName()})
		case pdpb.Uint8:
			uintProps = append(uintProps, cql.UintProp{Name: f.GetName(), ValLen: 1})
		case pdpb.Uint16:
			uintProps = append(uintProps, cql.UintProp{Name: f.GetName(), ValLen: 2})
		case pdpb.Uint32:
			uintProps = append(uintProps, cql.UintProp{Name: f.GetName(), ValLen: 4})
		case pdpb.Uint64:
			uintProps = append(uintProps, cql.UintProp{Name: f.GetName(), ValLen: 8})
		case pdpb.Float32:
			uintProps = append(uintProps, cql.UintProp{Name: f.GetName(), ValLen: 4, IsFloat: true})
		case pdpb.Float64:
			uintProps = append(uintProps, cql.UintProp{Name: f.GetName(), ValLen: 8, IsFloat: true})
		default:
			err = errors.Errorf("invalid filed type %+v of idxDef %+v", f.GetType().String(), idxDef)
			return
		}
	}
	docProt = &cql.DocumentWithIdx{
		Document: cql.Document{
			DocID:     0,
			UintProps: uintProps,
			StrProps:  strProps,
		},
		Index: idxDef.GetName(),
	}
	return
}

func convertToDocument(idxDef *pdpb.IndexDef, docID uint64, pairs []*raftcmdpb.FVPair) (doc *cql.DocumentWithIdx, err error) {
	doc = &cql.DocumentWithIdx{
		Document: cql.Document{
			DocID:     docID,
			UintProps: make([]cql.UintProp, 0),
			StrProps:  make([]cql.StrProp, 0),
		},
		Index: idxDef.GetName(),
	}
	log.Debugf("store-index: idxDef %+v, docID %+v, pairs %+v", idxDef, docID, pairs)
	for _, pair := range pairs {
		field := string(pair.GetField())
		valS := string(pair.GetValue())
		var val uint64
		for _, f := range idxDef.Fields {
			if f.GetName() != field {
				continue
			}
			switch f.GetType() {
			case pdpb.Text:
				doc.Document.StrProps = append(doc.Document.StrProps, cql.StrProp{Name: f.GetName(), Val: valS})
			case pdpb.Uint8:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 1})
			case pdpb.Uint16:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 2})
			case pdpb.Uint32:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 4})
			case pdpb.Uint64:
				if val, err = strconv.ParseUint(valS, 10, 64); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 8})
			case pdpb.Float32:
				if val, err = util.Float32ToSortableUint64(valS); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 4, IsFloat: true})
			case pdpb.Float64:
				if val, err = util.Float64ToSortableUint64(valS); err != nil {
					return
				}
				doc.Document.UintProps = append(doc.Document.UintProps, cql.UintProp{Name: f.GetName(), Val: val, ValLen: 8, IsFloat: true})
			default:
				err = errors.Errorf("invalid filed type %+v of idxDef %+v", f.GetType().String(), idxDef)
				return
			}
		}
	}
	return
}

//IndicesDiff is indices definion difference
type IndicesDiff struct {
	toDelete []*pdpb.IndexDef
	toCreate []*pdpb.IndexDef
}

//detect difference of indices and indicesNew
func diffIndices(indices, indicesNew map[string]*pdpb.IndexDef) (delta *IndicesDiff) {
	delta = &IndicesDiff{
		toDelete: make([]*pdpb.IndexDef, 0),
		toCreate: make([]*pdpb.IndexDef, 0),
	}
	var ok bool
	var name string
	var idxDefCur, idxDefNew *pdpb.IndexDef
	for name, idxDefCur = range indices {
		if idxDefNew, ok = indicesNew[name]; !ok {
			delta.toDelete = append(delta.toDelete, idxDefCur)
		} else if !reflect.DeepEqual(idxDefCur, idxDefNew) {
			delta.toDelete = append(delta.toDelete, idxDefCur)
			delta.toCreate = append(delta.toCreate, idxDefNew)
		}
	}
	for _, idxDefNew = range indicesNew {
		if _, ok := indices[idxDefNew.GetName()]; !ok {
			delta.toCreate = append(delta.toCreate, idxDefNew)
		}
	}
	return
}
