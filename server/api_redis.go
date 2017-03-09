package server

import (
	"fmt"

	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
)

func (s *Server) startRedisAPIServer() error {
	select {
	case <-s.stopC:
		return s.stopRedisAPIServer()
	default:
		return s.s.Start(s.doConnection)
	}
}

func (s *Server) stopRedisAPIServer() error {
	// TODO: 考虑一致性问题
	s.s.Stop()
	return nil
}

func (s *Server) doConnection(session goetty.IOSession) error {
	for {
		req, err := session.Read()
		if err != nil {
			return err
		}

		cmd, _ := req.(*redis.Command)
		fmt.Printf("cmd: %s", cmd.Cmd)
		for _, arg := range cmd.Args {
			fmt.Printf(" %s", string(arg))
		}
		fmt.Println("")

		// session.Write(redis.StatusReply("OK"))

		return nil
	}
}
