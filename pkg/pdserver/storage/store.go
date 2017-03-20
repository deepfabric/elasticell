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
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/pkg/errors"
)

const (
	// DefaultTimeout default timeout
	DefaultTimeout = time.Second * 3
	// DefaultRequestTimeout default request timeout
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSlowRequestTime default slow request time
	DefaultSlowRequestTime = time.Second * 1

	pdRootPath         = "/pd"
	pdIDPath           = "/pd/meta/id"
	pdLeaderPath       = "/pd/meta/leader"
	pdClusterIDPath    = "/pd/meta/cluster_id"
	pdBootstrappedPath = "/pd/meta/boot"
	pdClusterRootPath  = "/pd/cluster"
)

// Store used for  metedata
type Store struct {
	client *clientv3.Client
}

// NewStore create a store
func NewStore(cfg *embed.Config) (*Store, error) {
	c, err := initEctdClient(cfg)
	if err != nil {
		return nil, err
	}

	s := new(Store)
	s.client = c
	return s, nil
}

func initEctdClient(cfg *embed.Config) (*clientv3.Client, error) {
	endpoints := []string{cfg.LCUrls[0].String()}

	log.Infof("bootstrap: create etcd v3 client, endpoints=<%v>", endpoints)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: DefaultTimeout,
	})
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	return client, nil
}

// Close close ectd client
func (s *Store) Close() error {
	if s.client != nil {
		return s.client.Close()
	}

	return nil
}
