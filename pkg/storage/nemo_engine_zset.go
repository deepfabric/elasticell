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

// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	gonemo "github.com/deepfabric/go-nemo"
)

type nemoZSetEngine struct {
	db *gonemo.NEMO
}

func newNemoZSetEngine(db *gonemo.NEMO) ZSetEngine {
	return &nemoZSetEngine{
		db: db,
	}
}

func (e *nemoZSetEngine) ZAdd(key []byte, score float64, member []byte) (int64, error) {
	return e.db.ZAdd(key, score, member)
}

func (e *nemoZSetEngine) ZCard(key []byte) (int64, error) {
	return e.db.ZCard(key)
}

func (e *nemoZSetEngine) ZCount(key []byte, begin float64, end float64) (int64, error) {
	return e.db.ZCount(key, begin, end, false, false)
}

func (e *nemoZSetEngine) ZIncrBy(key []byte, member []byte, by float64) ([]byte, error) {
	return e.db.ZIncrby(key, member, by)
}

func (e *nemoZSetEngine) ZLexCount(key []byte, min []byte, max []byte) (int64, error) {
	return e.db.ZLexcount(key, min, max)
}

func (e *nemoZSetEngine) ZRange(key []byte, start int64, stop int64) ([]*raftcmdpb.ScorePair, error) {
	scores, values, err := e.db.ZRange(key, start, stop)
	if err != nil {
		return nil, err
	}

	l := len(scores)
	pairs := make([]*raftcmdpb.ScorePair, l)
	for i := 0; i < l; i++ {
		pairs[i] = &raftcmdpb.ScorePair{
			Score:  scores[i],
			Member: values[i],
		}
	}

	return pairs, nil
}

func (e *nemoZSetEngine) ZRangeByLex(key []byte, min []byte, max []byte) ([][]byte, error) {
	return e.db.ZRangebylex(key, min, max)
}

func (e *nemoZSetEngine) ZRangeByScore(key []byte, min float64, max float64) ([]*raftcmdpb.ScorePair, error) {
	scores, values, err := e.db.ZRangebyScore(key, min, max, false, false)
	if err != nil {
		return nil, err
	}

	l := len(scores)
	pairs := make([]*raftcmdpb.ScorePair, l)
	for i := 0; i < l; i++ {
		pairs[i] = &raftcmdpb.ScorePair{
			Score:  scores[i],
			Member: values[i],
		}
	}

	return pairs, nil
}

func (e *nemoZSetEngine) ZRank(key []byte, member []byte) (int64, error) {
	return e.db.ZRank(key, member)
}

func (e *nemoZSetEngine) ZRem(key []byte, members ...[]byte) (int64, error) {
	return e.db.ZRem(key, members...)
}

func (e *nemoZSetEngine) ZRemRangeByLex(key []byte, min []byte, max []byte) (int64, error) {
	return e.db.ZRemrangebylex(key, min, max, false, false)
}

func (e *nemoZSetEngine) ZRemRangeByRank(key []byte, start int64, stop int64) (int64, error) {
	return e.db.ZRemrangebyrank(key, start, stop)
}

func (e *nemoZSetEngine) ZRemRangeByScore(key []byte, min float64, max float64) (int64, error) {
	return e.db.ZRemrangebyscore(key, min, max, false, false)
}

func (e *nemoZSetEngine) ZScore(key []byte, member []byte) ([]byte, error) {
	exists, value, err := e.db.ZScore(key, member)
	if !exists {
		return nil, err
	}

	return util.FormatFloat64ToBytes(value), err
}
