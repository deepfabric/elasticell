package server

import (
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/fagongzi/goetty"
)

// Server a server provide kv cache based on redis protocol
type Server struct {
	sd    storage.Driver
	s     *goetty.Server
	stopC chan interface{}
}

// NewServer create a server use spec cfg
func NewServer(cfg *Cfg) *Server {
	return &Server{
		s: goetty.NewServerSize(cfg.Listen,
			decoder,
			encoder,
			cfg.ReadBufferSize,
			cfg.WriteBufferSize,
			goetty.NewInt64IDGenerator()),
		stopC: make(chan interface{}),
	}
}

// Start start the server
func (s *Server) Start() error {
	go s.startRedisAPIServer()
	return nil
}

// Stop stop the server
func (s *Server) Stop() error {
	return nil
}
