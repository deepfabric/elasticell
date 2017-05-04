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
	"io/ioutil"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

const (
	addrClient = "http://127.0.0.1:10000"
	addrPeer   = "http://127.0.0.1:10001"
	addrGRPC   = "127.0.0.1:10002"
)

// NewTestSingleServer returns a test single server
func NewTestSingleServer() *Server {
	return NewServer(NewTestSingleConfig())
}

// NewTestSingleConfig returns a test single server config
func NewTestSingleConfig() *Cfg {
	cfg := &Cfg{
		EmbedEtcd: &EmbedEtcdCfg{},
		Schedule:  &ScheduleCfg{},
	}

	cfg.Name = "test_pd"
	cfg.DataDir, _ = ioutil.TempDir("/tmp", cfg.Name)
	cfg.LeaseSecsTTL = 1
	cfg.RPCAddr = addrGRPC

	cfg.EmbedEtcd.ClientUrls = addrClient
	cfg.EmbedEtcd.PeerUrls = addrPeer
	cfg.EmbedEtcd.InitialCluster = fmt.Sprintf("test_pd=%s", cfg.EmbedEtcd.PeerUrls)
	cfg.EmbedEtcd.InitialClusterState = "new"

	cfg.Schedule.MaxReplicas = 3
	cfg.Schedule.LocationLabels = []string{"zone", "rack"}
	cfg.Schedule.MaxSnapshotCount = 3
	cfg.Schedule.MaxStoreDownTimeMs = 3600000
	cfg.Schedule.LeaderScheduleLimit = 16
	cfg.Schedule.CellScheduleLimit = 12
	cfg.Schedule.ReplicaScheduleLimit = 16

	return cfg
}

// NewMockStore returns a mock store
func NewMockStore() Store {
	return &mockStore{}
}

type mockStore struct {
	sync.RWMutex

	id uint64
}

func (s *mockStore) GetCurrentClusterMembers() (*clientv3.MemberListResponse, error) {
	return nil, nil
}

func (s *mockStore) GetClusterID() (uint64, error) {
	return 0, nil
}

func (s *mockStore) CreateFirstClusterID() (uint64, error) {
	return 0, nil
}

func (s *mockStore) SetClusterBootstrapped(clusterID uint64, cluster metapb.Cluster, store metapb.Store, cell metapb.Cell) (bool, error) {
	return false, nil
}

func (s *mockStore) LoadClusterMeta(clusterID uint64) (*metapb.Cluster, error) {
	return nil, nil
}

func (s *mockStore) LoadStoreMeta(clusterID uint64, limit int64, do func(metapb.Store)) error {
	return nil
}

func (s *mockStore) LoadCellMeta(clusterID uint64, limit int64, do func(metapb.Cell)) error {
	return nil
}

func (s *mockStore) SetStoreMeta(clusterID uint64, store metapb.Store) error {
	return nil
}

func (s *mockStore) SetCellMeta(clusterID uint64, cell metapb.Cell) error {
	return nil
}

func (s *mockStore) CampaignLeader(leaderSignature string, leaderLeaseTTL int64, enableLeaderFun func()) error {
	return nil
}

func (s *mockStore) WatchLeader() {

}

func (s *mockStore) ResignLeader(leaderSignature string) error {
	return nil
}

func (s *mockStore) GetCurrentLeader() (*pdpb.Leader, error) {
	return nil, nil
}

func (s *mockStore) GetID() (uint64, error) {
	s.Lock()
	defer s.Unlock()

	v := s.id
	s.id++

	return v, nil
}

func (s *mockStore) CreateID(leaderSignature string, value uint64) error {
	return nil
}

func (s *mockStore) UpdateID(leaderSignature string, old, value uint64) error {
	return nil
}

func (s *mockStore) Close() error {
	return nil
}
