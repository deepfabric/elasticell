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
	"bytes"
	"math"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	gonemo "github.com/deepfabric/go-nemo"
	"golang.org/x/net/context"
)

type nemoZSetEngine struct {
	limiter *util.Limiter
	db      *gonemo.NEMO
}

func newNemoZSetEngine(db *gonemo.NEMO, cfg *NemoCfg) ZSetEngine {
	return &nemoZSetEngine{
		limiter: util.NewLimiter(cfg.LimitConcurrencyWrite),
		db:      db,
	}
}

func (e *nemoZSetEngine) ZAdd(key []byte, score float64, member []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.ZAdd(key, score, member)
	e.limiter.Release()

	return n, err
}

func (e *nemoZSetEngine) ZCard(key []byte) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.ZCard(key)
	e.limiter.Release()

	return n, err
}

func (e *nemoZSetEngine) ZCount(key []byte, min []byte, max []byte) (int64, error) {
	minF, includeMin, err := parseInclude(min)
	if err != nil {
		return 0, err
	}

	maxF, includeMax, err := parseInclude(max)
	if err != nil {
		return 0, err
	}

	return e.db.ZCount(key, minF, maxF, includeMin, includeMax)
}

func (e *nemoZSetEngine) ZIncrBy(key []byte, member []byte, by float64) ([]byte, error) {
	e.limiter.Wait(context.TODO())
	value, err := e.db.ZIncrby(key, member, by)
	e.limiter.Release()

	return value, err

}

func (e *nemoZSetEngine) ZLexCount(key []byte, min []byte, max []byte) (int64, error) {
	min, includeMin := isInclude(min)
	max, includeMax := isInclude(max)

	return e.db.ZLexcount(key, min, max, includeMin, includeMax)
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
	min, includeMin := isInclude(min)
	max, includeMax := isInclude(max)

	if len(max) == 1 && max[0] == '+' {
		max[0] = byte('z' + 1)
	}

	return e.db.ZRangebylex(key, min, max, includeMin, includeMax)
}

func (e *nemoZSetEngine) ZRangeByScore(key []byte, min []byte, max []byte) ([]*raftcmdpb.ScorePair, error) {
	minF, includeMin, err := parseInclude(min)
	if err != nil {
		return nil, err
	}

	maxF, includeMax, err := parseInclude(max)
	if err != nil {
		return nil, err
	}

	scores, values, err := e.db.ZRangebyScore(key, minF, maxF, includeMin, includeMax)
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
	e.limiter.Wait(context.TODO())
	n, err := e.db.ZRem(key, members...)
	e.limiter.Release()

	return n, err
}

func (e *nemoZSetEngine) ZRemRangeByLex(key []byte, min []byte, max []byte) (int64, error) {
	min, includeMin := isInclude(min)
	max, includeMax := isInclude(max)

	e.limiter.Wait(context.TODO())
	n, err := e.db.ZRemrangebylex(key, min, max, includeMin, includeMax)
	e.limiter.Release()

	return n, err
}

func (e *nemoZSetEngine) ZRemRangeByRank(key []byte, start int64, stop int64) (int64, error) {
	e.limiter.Wait(context.TODO())
	n, err := e.db.ZRemrangebyrank(key, start, stop)
	e.limiter.Release()

	return n, err
}

func (e *nemoZSetEngine) ZRemRangeByScore(key []byte, min []byte, max []byte) (int64, error) {
	minF, includeMin, err := parseInclude(min)
	if err != nil {
		return 0, err
	}

	maxF, includeMax, err := parseInclude(max)
	if err != nil {
		return 0, err
	}

	e.limiter.Wait(context.TODO())
	n, err := e.db.ZRemrangebyscore(key, minF, maxF, includeMin, includeMax)
	e.limiter.Release()

	return n, err
}

func (e *nemoZSetEngine) ZScore(key []byte, member []byte) ([]byte, error) {
	exists, value, err := e.db.ZScore(key, member)
	if !exists {
		return nil, err
	}

	return util.FormatFloat64ToBytes(value), err
}

func isInclude(value []byte) ([]byte, bool) {
	include := value[0] != '('
	if value[0] == '(' || value[0] == '[' {
		value = value[1:]
	}

	return value, !include
}

var (
	max = []byte("+inf")
	min = []byte("-inf")
)

func parseInclude(value []byte) (float64, bool, error) {
	value, include := isInclude(value)
	if bytes.Compare(value, max) == 0 {
		return math.MaxFloat64, include, nil
	} else if bytes.Compare(value, min) == 0 {
		return math.SmallestNonzeroFloat64, include, nil
	}

	valueF, err := util.StrFloat64(value)
	return valueF, include, err
}
