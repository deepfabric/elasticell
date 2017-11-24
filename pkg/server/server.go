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

package server

import (
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/node"
	"github.com/deepfabric/elasticell/pkg/raftstore"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

// Server a server provide kv cache based on redis protocol
type Server struct {
	redisServer *RedisServer
	nodeServer  *node.Node

	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan interface{}

	runner *util.Runner
}

// NewServer create a server use spec cfg
func NewServer(cfg *Cfg) *Server {
	globalCfg = cfg

	s := new(Server)
	s.stopC = make(chan interface{})
	s.runner = util.NewRunner()

	s.initNode()
	s.initRedis()

	return s
}

// Start start the server
func (s *Server) Start() {
	util.InitMetric(s.runner, globalCfg.Metric)

	go s.listenToStop()

	store := s.startNode()
	s.startRedis(store)
}

// Stop stop the server
func (s *Server) Stop() {
	s.stopWG.Add(1)
	s.stopC <- ""
	s.stopWG.Wait()
}

func (s *Server) listenToStop() {
	<-s.stopC
	s.doStop()
}

func (s *Server) doStop() {
	s.stopOnce.Do(func() {
		defer s.stopWG.Done()

		s.runner.Stop()
		s.stopNode()
		s.stopRedis()
	})
}

func (s *Server) startRedis(store *raftstore.Store) {
	if nil != s.redisServer {
		s.redisServer.store = store
		err := s.redisServer.Start()
		if err != nil {
			log.Fatalf("bootstrap: failure to start redis server, cfg=<%v> errors:\n %+v",
				globalCfg,
				err)
			return
		}
	}

	log.Info("stop: stop redis server succ")
}

func (s *Server) stopRedis() {
	if nil != s.redisServer {
		err := s.redisServer.Stop()
		if err != nil {
			log.Errorf("stop: stop redis server failure, cfg=<%v> errors:\n %+v",
				globalCfg,
				err)
		}
	}
}

func (s *Server) startNode() *raftstore.Store {
	if nil != s.nodeServer {
		return s.nodeServer.Start()
	}

	return nil
}

func (s *Server) stopNode() {
	if nil != s.nodeServer {
		err := s.nodeServer.Stop()
		if err != nil {
			log.Errorf("stop: stop node failure, errors:\n %+v", err)
			return
		}
	}

	log.Info("stop: stop node succ")
}

func (s *Server) initRedis() {
	rs := new(RedisServer)
	rs.s = goetty.NewServerSize(globalCfg.AddrCli,
		redis.Decoder,
		redis.Encoder,
		globalCfg.BufferCliRead,
		globalCfg.BufferCliWrite,
		goetty.NewInt64IDGenerator())

	s.redisServer = rs
	s.redisServer.init()
}

func (s *Server) initNode() {
	driver, err := s.initDriver()
	if err != nil {
		log.Fatalf("bootstrap: init meta db failure, errors:\n %+v", err)
		return
	}

	n, err := node.NewNode(globalCfg.AddrCli, globalCfg.Node, driver)
	if err != nil {
		log.Fatalf("bootstrap: create node failure, errors:\n %+v", err)
		return
	}
	s.nodeServer = n
}

func (s *Server) initDriver() (storage.Driver, error) {
	cfg := &storage.NemoCfg{
		DataPath:              globalCfg.Node.RaftStore.DataPath,
		OptionPath:            globalCfg.Node.RaftStore.OptionPath,
		LimitConcurrencyWrite: globalCfg.Node.RaftStore.LimitConcurrencyWrite,
	}
	return storage.NewNemoDriver(cfg)
}
