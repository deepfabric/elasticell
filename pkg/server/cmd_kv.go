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
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
)

func (s *Server) setCmd(cmd *redis.Command, session goetty.IOSession) error {
	if len(cmd.Args) != 2 {
		return ErrInvlidArgs
	}

	// return s.sd.Set(cmd.Args[0], cmd.Args[1])
	return nil
}

func (s *Server) getCmd(cmd *redis.Command, session goetty.IOSession) error {
	if len(cmd.Args) != 1 {
		return ErrInvlidArgs
	}

	// v, err := s.sd.Get(cmd.Args[0])

	return nil
}
