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

package storage

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/pkg/errors"
)

func (s *Store) getValue(key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := s.get(key, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

func (s *Store) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(s.client).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("embed-ectd: read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, errors.Wrap(err, "")
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("embed-ectd: read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}
