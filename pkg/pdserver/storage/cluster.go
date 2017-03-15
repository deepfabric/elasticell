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
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

// GetCurrentClusterMembers return members in current etcd cluster
func (s *Store) GetCurrentClusterMembers() (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	members, err := s.client.MemberList(ctx)
	cancel()

	return members, errors.Wrap(err, "")
}

// GetClusterID get current cluster id
// if cluster is not init, return 0
func (s *Store) GetClusterID() (uint64, error) {
	resp, err := s.get(pdClusterIDPath, clientv3.WithFirstCreate()...)

	if len(resp.Kvs) == 0 {
		return 0, err
	}

	key := string(resp.Kvs[0].Key)

	// If the key is "pdClusterIDPath", parse the cluster ID from it.
	if key == pdClusterIDPath {
		return util.BytesToUint64(resp.Kvs[0].Value)
	}

	// Parse the cluster ID from any other keys for compatibility.
	elems := strings.Split(key, "/")
	if len(elems) < 4 {
		return 0, errors.Errorf("invalid cluster key %v", key)
	}

	return strconv.ParseUint(elems[3], 10, 64)
}

// CreateFirstClusterID create the first cluster
// More than one pd instance do this operation at the first time,
// only one can succ,
// others will get the committed id.
func (s *Store) CreateFirstClusterID() (uint64, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultTimeout)
	defer cancel()

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
	value := util.Uint64ToBytes(clusterID)

	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(pdClusterIDPath), "=", 0)).
		Then(clientv3.OpPut(pdClusterIDPath, string(value))).
		Else(clientv3.OpGet(pdClusterIDPath)).
		Commit()

	if err != nil {
		return 0, errors.Wrap(err, "")
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errors.Errorf("txn returns empty response: %v", resp)
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.Errorf("txn returns invalid range response: %v", resp)
	}

	return util.BytesToUint64(response.Kvs[0].Value)
}

// IsClusterBootstrapped check the cluster is bootstrap,
// if not the kv node will create first cell.
func (s *Store) IsClusterBootstrapped() (bool, error) {
	v, err := s.getValue(pdBootstrappedPath)
	if err != nil {
		return false, err
	}

	return v != nil, nil
}

// SetClusterBootstrapped set cluster bootstrapped flag, only one can succ.
func (s *Store) SetClusterBootstrapped() (bool, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultTimeout)
	defer cancel()

	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(pdBootstrappedPath), "=", 0)).
		Then(clientv3.OpPut(pdBootstrappedPath, "boot")).
		Commit()

	if err != nil {
		return false, errors.Wrap(err, "")
	}

	// Txn commits ok, return  succ.
	if resp.Succeeded {
		return true, nil
	}

	// Otherwise, other succ
	return false, nil
}
