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
	"net"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func (s *Server) startRPC() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("rpc: crash, errors:\n %+v", err)
		}
	}()

	lis, err := net.Listen("tcp", s.cfg.AddrRPC)
	if err != nil {
		log.Fatalf("bootstrap: start grpc server failure, listen=<%s> errors:\n %+v",
			s.cfg.AddrRPC,
			err)
		return
	}

	s.rpcServer = grpc.NewServer()
	pdpb.RegisterPDServiceServer(s.rpcServer, NewRPCHandler(s))
	reflection.Register(s.rpcServer)

	if err := s.rpcServer.Serve(lis); err != nil {
		if !s.callStop {
			log.Fatalf("bootstrap: start grpc server failure, listen=<%s> errors:\n %+v",
				s.cfg.AddrRPC,
				err)
		}

		return
	}

	log.Infof("stop: grpc server stopped, addr=<%s>", s.cfg.AddrRPC)
}

func (s *Server) closeRPC() {
	if s.rpcServer != nil {
		s.rpcServer.GracefulStop()
	}
}
