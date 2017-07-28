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
	"sync"
	"sync/atomic"

	"github.com/coreos/etcd/embed"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pd"
	"google.golang.org/grpc"
)

// Server the pd server
type Server struct {
	cfg *Cfg

	// ectd fields
	id   uint64
	etcd *embed.Etcd

	// rpc fields
	rpcServer *grpc.Server

	store Store

	// cluster fields
	isLeaderValue   int64
	leaderSignature string
	clusterID       uint64
	cluster         *CellCluster
	leaderProxy     *pd.Client
	leaderMux       sync.RWMutex
	idAlloc         *idAllocator

	// status
	callStop bool
	closed   int64

	// stop fields
	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan interface{}

	complete chan struct{}
}

// NewServer create a pd server
func NewServer(cfg *Cfg) *Server {
	s := new(Server)
	s.cfg = cfg
	s.stopC = make(chan interface{})
	s.isLeaderValue = 0
	s.complete = make(chan struct{})

	return s
}

// Name returns name of current pd server
func (s *Server) Name() string {
	return s.cfg.Name
}

// Start start the pd server
func (s *Server) Start() {
	go s.listenToStop()
	go s.startRPC()

	s.startEmbedEtcd()

	s.initCluster()

	s.setServerIsStarted()
	go s.startLeaderLoop()

	<-s.complete
	close(s.complete)
	s.complete = nil
}

// Stop the server
func (s *Server) Stop() {
	s.stopWG.Add(1)
	s.stopC <- ""
	s.stopWG.Wait()
}

func (s *Server) listenToStop() {
	<-s.stopC
	defer s.stopWG.Done()
	s.doStop()
}

func (s *Server) doStop() {
	s.stopOnce.Do(func() {
		s.callStop = true
		s.closeRPC()
		s.closeEmbedEtcd()
		s.setServerIsStopped()
	})
}

func (s *Server) notifyElectionComplete() {
	if s.complete != nil {
		s.complete <- struct{}{}
	}
}

// GetCfg returns cfg, just for test
func (s *Server) GetCfg() *Cfg {
	return s.cfg
}

func (s *Server) initCluster() {
	clusterID, err := s.store.GetClusterID()
	if err != nil {
		log.Fatalf("bootstrap: get cluster id failure, errors:\n %+v", err)
		return
	}

	log.Infof("bootstrap: get cluster id, clusterID=<%d>", clusterID)

	if clusterID == pd.ZeroID {
		clusterID, err = s.store.CreateFirstClusterID()
		if err != nil {
			log.Fatalf("bootstrap: create first cluster id failure, errors:\n %+v", err)
			return
		}

		log.Infof("bootstrap: first clusterID created, clusterID=<%d>", clusterID)
	}

	s.clusterID = clusterID

	s.idAlloc = newIDAllocator(s.store, func() string {
		return s.leaderSignature
	})

	s.cluster = newCellCluster(s)
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.closed) == 1
}

func (s *Server) setServerIsStopped() {
	atomic.StoreInt64(&s.closed, 1)
}

func (s *Server) setServerIsStarted() {
	atomic.StoreInt64(&s.closed, 0)
}
