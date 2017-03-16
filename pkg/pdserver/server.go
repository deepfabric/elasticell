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
	"github.com/deepfabric/elasticell/pkg/pdserver/storage"
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

	store *storage.Store

	// cluster fields
	isLeaderValue   int64
	leaderSignature string
	clusterID       uint64
	cluster         *CellCluster
	leaderProxy     *pd.Client
	leaderProxyMut  sync.RWMutex
	idAlloc         *idAllocator

	// status
	closed int64

	// stop fields
	stopOnce *sync.Once
	stopWG   *sync.WaitGroup
	stopC    chan interface{}
}

// NewServer create a pd server
func NewServer(cfg *Cfg) *Server {
	s := new(Server)
	s.cfg = cfg
	s.stopC = make(chan interface{})
	s.stopOnce = new(sync.Once)
	s.stopWG = new(sync.WaitGroup)
	s.isLeaderValue = 0

	return s
}

// Start start the pd server
func (s *Server) Start() {
	s.printStartENV()

	go s.listenToStop()

	s.startEmbedEtcd()
	s.initCluster()

	go s.startRPC()

	s.setServerIsStarted()
	go s.startLeaderLoop()
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
		s.closeRPC()
		s.closeEmbedEtcd()
		s.setServerIsStopped()
	})
}

func (s *Server) printStartENV() {
	// TODO: print env
	// info := `
	//                     PD Server
	// ----------------------------------------------------
	// Version: %s
	// OS     : %s
	// Cfg    : %v
	// `

	// log.Infof(info,
	// Version,
	// host.GetOSInfo())
}

func (s *Server) initCluster() {
	clusterID, err := s.store.GetClusterID()
	if err != nil {
		log.Fatalf("bootstrap: get cluster id failure, errors:\n %+v", err)
		return
	}

	log.Infof("bootstrap: get cluster id, clusterID=<%d>", clusterID)

	if clusterID == 0 {
		clusterID, err = s.store.CreateFirstClusterID()
		if err != nil {
			log.Fatalf("bootstrap: create first cluster id failure, errors:\n %+v", err)
			return
		}

		log.Infof("bootstrap: first clusterID created, clusterID=<%d>", clusterID)
	}

	s.clusterID = clusterID

	s.idAlloc = newIDAllocator(s)
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
