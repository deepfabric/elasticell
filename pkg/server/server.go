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
	"github.com/deepfabric/elasticell/pkg/storage"
)

// Server a server provide kv cache based on redis protocol
type Server struct {
	cfg *Cfg

	sd storage.Driver

	redisServer *RedisServer
	nodeServer  *node.Node

	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan interface{}
}

// NewServer create a server use spec cfg
func NewServer(cfg *Cfg) *Server {
	s := new(Server)
	s.redisServer = newRedisServer(cfg)

	cfg.Node.StoreAddr = cfg.Redis.Listen
	n, err := node.NewNode(cfg.Node)
	if err != nil {
		log.Fatalf("bootstrap: bootstrap failure, errors:\n %+v", err)
		return nil
	}
	s.nodeServer = n
	s.stopC = make(chan interface{})

	return s
}

// Start start the server
func (s *Server) Start() {
	go s.listenToStop()

	go s.startRedis()
	go s.startNode()
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
		s.stopRedis()
		s.stopNode()
	})
}

func (s *Server) startRedis() {
	if nil != s.redisServer {
		err := s.redisServer.Start()
		if err != nil {
			log.Fatalf("bootstrap: failure to start redis server, cfg=<%v> errors:\n %+v",
				s.cfg,
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
				s.cfg,
				err)
		}
	}
}

func (s *Server) startNode() {
	if nil != s.nodeServer {
		s.nodeServer.Start()
	}
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
