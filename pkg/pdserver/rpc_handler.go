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
	"errors"

	"github.com/deepfabric/elasticell/pkg/log"
	pb "github.com/deepfabric/elasticell/pkg/pdpb"
	"golang.org/x/net/context"
)

var (
	errNotLeader = errors.New("not leader")
)

// RPCHandler it's a grpc interface implemention
type RPCHandler struct {
	server *Server
}

// NewRPCHandler create a new instance
func NewRPCHandler(server *Server) pb.PDServiceServer {
	return &RPCHandler{
		server: server,
	}
}

// GetLeader get current leader
func (h *RPCHandler) GetLeader(c context.Context, req *pb.LeaderReq) (*pb.LeaderRsp, error) {
	log.Debugf("rpc: get a req, type=<%s> req=<%v>", "LeaderReq", req)

	leader, err := h.server.store.GetCurrentLeader()
	if err != nil {
		return nil, err
	}

	return &pb.LeaderRsp{
		Leader: *leader,
	}, nil
}

// IsClusterBootstrap check cluster is bootstrap already
func (h *RPCHandler) IsClusterBootstrap(c context.Context, req *pb.IsClusterBootstrapReq) (*pb.IsClusterBootstrapRsp, error) {
	log.Debugf("rpc: get a req<%s-%d>, type=<%s> req=<%v>",
		req.From,
		req.Id,
		"IsClusterBootstrapReq",
		req)

	// forward to leader
	if !h.server.IsLeader() {
		proxy := h.server.GetLeaderProxy()
		if nil == proxy {
			return nil, errNotLeader
		}

		log.Debugf("rpc: forward a req<%s-%d>, target=<%s>",
			req.From,
			req.Id,
			proxy.GetLastPD())
		return proxy.IsClusterBootstrapped(c, req)
	}

	return &pb.IsClusterBootstrapRsp{
		Value: h.server.isClusterBootstrapped(),
	}, nil
}

// BootstrapCluster bootstrap cluster
func (h *RPCHandler) BootstrapCluster(c context.Context, req *pb.BootstrapClusterReq) (*pb.BootstrapClusterRsp, error) {
	log.Debugf("rpc: get a req<%s-%d>, type=<%s> req=<%v>",
		req.From,
		req.Id,
		"BootstrapClusterReq",
		req)

	// forward to leader
	if !h.server.IsLeader() {
		proxy := h.server.GetLeaderProxy()
		if nil == proxy {
			return nil, errNotLeader
		}

		log.Debugf("rpc: forward a req<%s-%d>, target=<%s>",
			req.From,
			req.Id,
			proxy.GetLastPD())
		return proxy.BootstrapCluster(c, req)
	}

	return h.server.bootstrapCluster(req)
}
