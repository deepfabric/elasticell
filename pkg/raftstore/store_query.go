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
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/deepfabric/bkdtree"
	datastructures "github.com/deepfabric/go-datastructures"
	"github.com/deepfabric/indexer"

	"github.com/deepfabric/elasticell/pkg/pb/errorpb"
	"github.com/deepfabric/elasticell/pkg/pb/querypb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/pkg/errors"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/indexer/cql"
	"golang.org/x/net/context"
)

func (s *Store) readyToServeQuery(ctx context.Context) {
	var qrc *QueryRequestCb
	var state *QueryState
	var storeIDs []uint64
	var storeID uint64
	var ok bool
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Infof("store-query[%d]: readyToServeQuery stopped", s.GetID())
			return
		case qrc = <-s.queryReqChan:
			log.Debugf("store-query[%d]: got request %+v", s.GetID(), qrc)
			if qrc.cb != nil { //qrc from elasticell-proxy
				// TODO(yzc): make default limit config-able?
				limit := int(qrc.qr.Limit)
				if limit > 1000 {
					limit = 1000
					qrc.qr.Limit = uint64(limit)
				}
				state = &QueryState{
					qrc:            qrc,
					docs:           make([]*querypb.Document, 0, limit),
					docsOa:         datastructures.NewOrderedArray(limit),
					cellIDToStores: make(map[uint64][]uint64),
					storeIDToCells: make(map[uint64][]uint64),
					doneCells:      make([]uint64, 0),
					errorResults:   make([][]byte, 0),
				}
				s.rwlock.RLock()
				for k, v := range s.cellIDToStores {
					state.cellIDToStores[k] = v
				}
				for k, v := range s.storeIDToCells {
					state.storeIDToCells[k] = v
				}
				s.rwlock.RUnlock()
				s.queryStates[string(qrc.qr.UUID)] = state

				for storeID, cellIDs := range state.storeIDToCells {
					req := &querypb.QueryReq{
						UUID:      qrc.qr.UUID,
						ToStore:   storeID,
						FromStore: s.GetID(),
						Cells:     cellIDs,
						Index:     qrc.qr.Index,
						UintPreds: qrc.qr.UintPreds,
						StrPreds:  qrc.qr.StrPreds,
						Limit:     qrc.qr.Limit,
						OrderBy:   qrc.qr.OrderBy,
					}
					s.trans.sendQuery(req)
					log.Debugf("store-query[%d]: sent querypb.QueryReq %+v", s.GetID(), req)
				}
			} else { //qrc from store
				// query on local cells
				idxers := make([]*indexer.Indexer, 0)
				goneCellIDs := make([]uint64, 0)
				normCellIDs := make([]uint64, 0)
				for _, cellID := range qrc.qr.Cells {
					s.rwlock.RLock()
					var idxerExt *IndexerExt
					if idxerExt, ok = s.indexers[cellID]; !ok {
						goneCellIDs = append(goneCellIDs, cellID)
					} else {
						normCellIDs = append(normCellIDs, cellID)
						idxers = append(idxers, idxerExt.Indexer)
					}
					s.rwlock.RUnlock()
				}
				if len(goneCellIDs) != 0 {
					// send query response for gone cells
					err := new(errorpb.CellNotFound)
					rsp := &querypb.QueryRsp{
						UUID:      qrc.qr.UUID,
						ToStore:   qrc.qr.FromStore,
						FromStore: s.GetID(),
						Cells:     goneCellIDs,
						Error: &errorpb.Error{
							CellNotFound: err,
						},
					}
					s.trans.sendQuery(rsp)
					log.Debugf("store-query[%d]: sent querypb.QueryRsp %+v", s.GetID(), rsp)
				}
				if len(idxers) != 0 {
					var storeQR, cellQR *indexer.QueryResult
					storeQR = indexer.NewQueryResult(int(qrc.qr.Limit))
					cs := convertToCql(qrc.qr)
					for _, idxer := range idxers {
						if cellQR, err = idxer.Select(cs); err != nil {
							log.Errorf("store-query[%d]: indexer.Select failed under %+v with error %+v", s.GetID(), idxer.MainDir, err)
							continue
						}
						storeQR.Merge(cellQR)
					}
					// send query response for normal cells
					docs := make([]*querypb.Document, 0, int(qrc.qr.Limit))
					var doc *querypb.Document
					if qrc.qr.OrderBy == "" {
						bits := storeQR.Bm.Bits()
						cnt := len(bits)
						if cnt > int(qrc.qr.Limit) {
							cnt = int(qrc.qr.Limit)
						}
						for i := 0; i < cnt; i++ {
							docID := bits[i]
							if doc, err = s.readDocument(docID); err != nil {
								log.Errorf("store-query[%d]: readDocument(%+v) failed with error %+v", s.GetID(), docID, err)
								continue
							}
							docs = append(docs, doc)
						}
					} else {
						for _, item := range storeQR.Oa.Finalize() {
							point := item.(bkdtree.Point)
							docID := point.UserData
							if doc, err = s.readDocument(docID); err != nil {
								log.Errorf("store-query[%d]: readDocument(%+v) failed with error %+v", s.GetID(), docID, err)
								continue
							}
							doc.Order = point.Vals
							docs = append(docs, doc)
						}
					}
					rsp := &querypb.QueryRsp{
						UUID:      qrc.qr.UUID,
						ToStore:   qrc.qr.FromStore,
						FromStore: s.GetID(),
						Cells:     normCellIDs,
						Docs:      docs,
					}
					s.trans.sendQuery(rsp)
					log.Debugf("store-query[%d]: sent querypb.QueryRsp %+v", s.GetID(), rsp)
				}
			}
		case rsp := <-s.queryRspChan:
			log.Debugf("store-query[%d]: got response %+v", s.GetID(), rsp)
			if state, ok = s.queryStates[string(rsp.UUID)]; !ok {
				log.Infof("store-query[%d]: invalid UUID of rsp %+v", s.GetID(), rsp)
			} else if rsp.Error != nil {
				log.Infof("store-query[%d]: query failed on store %d cells %+v, error %+v", s.GetID(), rsp.FromStore, rsp.Cells, rsp.Error)
				for _, cellID := range rsp.Cells {
					if storeIDs, ok = state.cellIDToStores[cellID]; !ok {
						log.Errorf("store-query[%d]: unknown cell %d", s.GetID(), cellID)
					} else if len(storeIDs) > 0 {
						// try to query this cell on another store
						storeID = storeIDs[0]
						storeIDs = storeIDs[1:]
						state.cellIDToStores[cellID] = storeIDs
						req := &querypb.QueryReq{
							UUID:      rsp.UUID,
							ToStore:   storeID,
							FromStore: s.GetID(),
							Cells:     []uint64{cellID},
							Index:     state.qrc.qr.Index,
							UintPreds: state.qrc.qr.UintPreds,
							StrPreds:  state.qrc.qr.StrPreds,
							Limit:     state.qrc.qr.Limit,
							OrderBy:   state.qrc.qr.OrderBy,
						}
						s.trans.sendQuery(req)
						log.Debugf("store-query[%d]: sent querypb.QueryReq %+v", s.GetID(), req)
					} else {
						errMsg := fmt.Sprintf("cell %d failed on all stores", cellID)
						state.errorResults = append(state.errorResults, []byte(errMsg))
						state.doneCells = append(state.doneCells, cellID)
						log.Infof("store-query[%d]: query cell %d failed on all stores", s.GetID(), cellID)
					}
				}
			} else {
				limit := int(state.qrc.qr.Limit)
				needOrder := false
				if state.qrc.qr.OrderBy != "" {
					needOrder = true
				}
				if needOrder {
					for _, doc := range rsp.Docs {
						docPtr := DocPtr{doc}
						state.docsOa.Put(docPtr)
					}
				} else {
					state.docs = append(state.docs, rsp.Docs...)
					if len(state.docs) > limit {
						state.docs = state.docs[:limit]
					}
				}
				state.doneCells = append(state.doneCells, rsp.Cells...)
			}
			if len(state.cellIDToStores) == len(state.doneCells) || (state.qrc.qr.OrderBy == "" && len(state.docs) >= int(state.qrc.qr.Limit)) {
				// all cells has done the query, or has collected enough docs.
				// build and send response to redicell-proxy
				if state.qrc.qr.OrderBy != "" {
					docItems := state.docsOa.Finalize()
					num := len(docItems)
					state.docs = make([]*querypb.Document, num, num)
					for i := 0; i < num; i++ {
						state.docs[i] = docItems[i].(DocPtr).Document
					}
				}
				has := true
				rsp := pool.AcquireResponse()
				rsp.UUID = state.qrc.qr.UUID
				rsp.SessionID = state.qrc.qr.SessionID
				rsp.HasEmptyDocArrayResult = &has
				rsp.DocArrayResult = state.docs
				rsp.ErrorResults = state.errorResults
				resp := pool.AcquireRaftCMDResponse()
				resp.Responses = append(resp.Responses, rsp)
				buildUUID(state.qrc.qr.UUID, resp)
				log.Debugf("store-query[%d]: sent final response %+v", s.GetID(), rsp)
				state.qrc.cb(resp)
				delete(s.queryStates, string(state.qrc.qr.UUID))
			}
		}
	}
}

func (s *Store) readDocument(docID uint64) (doc *querypb.Document, err error) {
	var fvPairs []*raftcmdpb.FVPair
	var dataKey []byte
	if dataKey, err = s.engine.GetKVEngine().Get(getDocIDKey(docID)); err != nil {
		return
	}
	if len(dataKey) == 0 {
		err = errors.Errorf("docID %+v doesn't map to any key", docID)
		return
	}
	doc = &querypb.Document{
		Key: getOriginKey(dataKey),
	}
	if fvPairs, err = s.engine.GetHashEngine().HGetAll(dataKey); err != nil {
		return
	}
	for _, fv := range fvPairs {
		doc.FvPairs = append(doc.FvPairs, fv.Field)
		doc.FvPairs = append(doc.FvPairs, fv.Value)
	}
	log.Debugf("store-query[%d]: readDocument(%d) returned %+v", s.GetID(), docID, doc)
	return
}

func (s *Store) handleQueryRequest(qr *querypb.QueryReq, cb func(*raftcmdpb.RaftCMDResponse)) {
	qrc := &QueryRequestCb{
		qr: qr,
		cb: cb,
	}
	s.queryReqChan <- qrc
}

func (s *Store) handleQueryRsp(qcr *querypb.QueryRsp) {
	s.queryRspChan <- qcr
}

func (s *Store) refreshRangesLoop(ctx context.Context) {
	//TODO(yzc): make refresh configable?
	tickChan := time.Tick(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Infof("store-query[%d]: refreshRangesLoop stopped", s.GetID())
			return
		case <-tickChan:
			if err := s.refreshRanges(); err != nil {
				log.Errorf("store-query[%d]: refreshRanges failed with error\n%+v", s.GetID(), err)
			}
		}
	}
}

//adapted from elasticell-proxy (*RedisProxy).refreshRanges()
func (s *Store) refreshRanges() (err error) {
	var glrRsp *pdpb.GetLastRangesRsp
	var ok bool
	old := s.getSyncEpoch()
	log.Infof("store-query[%d]: try to sync, epoch=<%d>", s.GetID(), old)

	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	if old < s.syncEpoch {
		log.Infof("store-query[%d]: already sync, skip, old=<%d> new=<%d>", s.GetID(), old, s.syncEpoch)
		return
	}

	glrRsp, err = s.pdClient.GetLastRanges(context.TODO(), &pdpb.GetLastRangesReq{})
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}

	s.cellIDToStores = make(map[uint64][]uint64)
	s.storeIDToCells = make(map[uint64][]uint64)
	var cellIDs []uint64
	for _, r := range glrRsp.Ranges {
		cellID := r.Cell.ID
		storeID := r.LeaderStore.ID
		storeIDs := make([]uint64, 0)
		for _, peer := range r.Cell.Peers {
			if peer.StoreID != storeID {
				storeIDs = append(storeIDs, peer.StoreID)
			}
		}
		s.cellIDToStores[cellID] = storeIDs
		if cellIDs, ok = s.storeIDToCells[storeID]; !ok {
			cellIDs = make([]uint64, 0)
		}
		cellIDs = append(cellIDs, cellID)
		s.storeIDToCells[storeID] = cellIDs
	}

	s.syncEpoch++

	log.Infof("store-query[%d]: sync complete, epoch=%d", s.GetID(), s.syncEpoch)
	return
}

func (s *Store) getSyncEpoch() uint64 {
	return atomic.LoadUint64(&s.syncEpoch)
}

//convert raftcmdpb.Quest to cql.CqlSelect and querypb.QueryReq. assumes req.Cmd[0] is "query".
func convertToQueryReq(req *raftcmdpb.Request, docProts map[string]*cql.Document) (qr *querypb.QueryReq, err error) {
	var res interface{}
	cqlS := string(bytes.Join(req.Cmd, []byte{0x20}))
	res, err = cql.ParseCql(cqlS, docProts)
	if err != nil {
		return
	}
	switch r := res.(type) {
	case *cql.CqlSelect:
		qr = &querypb.QueryReq{
			UUID:      req.UUID,
			SessionID: req.SessionID,
			Index:     r.Index,
			OrderBy:   r.OrderBy,
			Limit:     uint64(r.Limit),
		}
		for _, uintPred := range r.UintPreds {
			qr.UintPreds = append(qr.UintPreds, &querypb.UintPred{Name: uintPred.Name, Low: uintPred.Low, High: uintPred.High})
		}
		for _, strPred := range r.StrPreds {
			qr.StrPreds = append(qr.StrPreds, &querypb.StrPred{Name: strPred.Name, ContWord: strPred.ContWord})
		}
	default:
		err = errors.Errorf("invalid query clause: %+v", cqlS)
		return
	}
	return
}

//convert querypb.QueryReq to cql.CqlSelect.
func convertToCql(qr *querypb.QueryReq) (cs *cql.CqlSelect) {
	cs = &cql.CqlSelect{
		Index:     qr.Index,
		OrderBy:   qr.OrderBy,
		Limit:     int(qr.Limit),
		UintPreds: make(map[string]cql.UintPred),
		StrPreds:  make(map[string]cql.StrPred),
	}
	for _, uintPred := range qr.UintPreds {
		cs.UintPreds[uintPred.Name] = cql.UintPred{Name: uintPred.Name, Low: uintPred.Low, High: uintPred.High}
	}
	for _, strPred := range qr.StrPreds {
		cs.StrPreds[strPred.Name] = cql.StrPred{Name: strPred.Name, ContWord: strPred.ContWord}
	}
	return
}
