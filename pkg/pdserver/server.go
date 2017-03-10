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

	"github.com/coreos/etcd/embed"
	"github.com/deepfabric/elasticell/pkg/pdserver/storage"
)

// Server the pd server
type Server struct {
	id uint64

	cfg  *Cfg
	etcd *embed.Etcd

	store *storage.Store

	stopOnce *sync.Once
	stopC    chan interface{}
}

// NewServer create a pd server
func NewServer(cfg *Cfg) *Server {
	s := new(Server)
	s.cfg = cfg
	s.stopC = make(chan interface{})
	s.stopOnce = new(sync.Once)
	return s
}

// Start start the pd server
func (s *Server) Start() {
	s.printStartENV()
	s.startEmbedEtcd()
	return
}

// Stop the server
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.doStop()
	})
}

func (s *Server) doStop() {
	// TODO: release resources
	s.closeEmbedEtcd()
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
