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

package pdserver

import (
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/pkg/errors"
)

// ListIndex list indices definion
func (s *pdStore) ListIndex() (idxDefs []*pdpb.IndexDef, err error) {
	var resp *clientv3.GetResponse
	key := s.getIndicesKey()
	if resp, err = s.get(key, clientv3.WithPrefix()); err != nil {
		return
	}
	idxDefs = make([]*pdpb.IndexDef, 0)
	for _, item := range resp.Kvs {
		v := &pdpb.IndexDef{}
		if err = v.Unmarshal(item.Value); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		idxDefs = append(idxDefs, v)
	}
	return
}

// GetIndex returns index definion
func (s *pdStore) GetIndex(id string) (idxDef *pdpb.IndexDef, err error) {
	var resp *clientv3.GetResponse
	key := s.getIndexKey(id)
	if resp, err = s.get(key); err != nil {
		return
	}
	if len(resp.Kvs) != 1 {
		err = errors.Errorf("resp.Kvs size is incorrect, want %d, have %d", 1, len(resp.Kvs))
		return
	}
	idxDef = &pdpb.IndexDef{}
	if err = idxDef.Unmarshal(resp.Kvs[0].Value); err != nil {
		err = errors.Wrap(err, "")
		return
	}
	return
}

//CreateIndex creates index
func (s *pdStore) CreateIndex(idxDef *pdpb.IndexDef) (err error) {
	var value []byte
	key := s.getIndexKey(idxDef.Name)
	if value, err = idxDef.Marshal(); err != nil {
		return errors.Wrap(err, "")
	}
	err = s.create(key, string(value))
	return
}

//DeleteIndex deletes index
func (s *pdStore) DeleteIndex(id string) (err error) {
	key := s.getIndexKey(id)
	err = s.delete(key)
	return
}

func (s *pdStore) getIndicesKey() string {
	return pdIndicesRootPath
}

func (s *pdStore) getIndexKey(id string) string {
	return fmt.Sprintf("%s/%s", pdIndicesRootPath, id)
}
