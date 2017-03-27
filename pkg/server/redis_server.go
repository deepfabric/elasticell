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
	"fmt"

	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
)

// RedisServer is provide a redis like server
type RedisServer struct {
	s *goetty.Server
}

// Start used for start the redis server
func (s *RedisServer) Start() error {
	return s.s.Start(s.doConnection)
}

// Stop is used for stop redis server
func (s *RedisServer) Stop() error {
	// TODO: 考虑一致性问题
	s.s.Stop()
	return nil
}

func (s *RedisServer) doConnection(session goetty.IOSession) error {
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
