package server

import (
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
)

func (s *Server) setCmd(cmd *redis.Command, session goetty.IOSession) error {
	if len(cmd.Args) != 2 {
		return ErrInvlidArgs
	}

	return s.sd.Set(cmd.Args[0], cmd.Args[1])
}

func (s *Server) getCmd(cmd *redis.Command, session goetty.IOSession) error {
	if len(cmd.Args) != 1 {
		return ErrInvlidArgs
	}

	// v, err := s.sd.Get(cmd.Args[0])

	return nil
}
