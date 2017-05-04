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
	"github.com/coreos/etcd/clientv3"
	"github.com/deepfabric/elasticell/pkg/util"
)

// GetID returns current id
func (s *pdStore) GetID() (uint64, error) {
	resp, err := s.getValue(pdIDPath)
	if err != nil {
		return 0, err
	}

	if resp == nil {
		return 0, nil
	}

	return util.BytesToUint64(resp)
}

// CreateID create id alloc info.
func (s *pdStore) CreateID(leaderSignature string, value uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(pdIDPath), "=", 0)
	op := clientv3.OpPut(pdIDPath, string(util.Uint64ToBytes(value)))
	resp, err := s.leaderTxn(leaderSignature, cmp).Then(op).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errMaybeNotLeader
	}

	return nil
}

// UpdateID update id for alloc.
func (s *pdStore) UpdateID(leaderSignature string, old, value uint64) error {
	cmp := clientv3.Compare(clientv3.Value(pdIDPath), "=", string(util.Uint64ToBytes(old)))
	op := clientv3.OpPut(pdIDPath, string(util.Uint64ToBytes(value)))
	resp, err := s.leaderTxn(leaderSignature, cmp).Then(op).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errMaybeNotLeader
	}

	return nil
}
