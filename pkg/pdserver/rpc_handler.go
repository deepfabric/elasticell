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
	"github.com/deepfabric/elasticell/pkg/pd"
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

// AllocID returns alloc id for kv node
func (h *RPCHandler) AllocID(c context.Context, req *pb.AllocIDReq) (*pb.AllocIDRsp, error) {
	doFun := func() (interface{}, error) {
		id, err := h.server.idAlloc.newID()
		if err != nil {
			return nil, err
		}

		return &pb.AllocIDRsp{
			Id: id,
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.AllocID(c, req)
	}

	tmp, err := h.doHandle("AllocID", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	rsp, _ := tmp.(*pb.AllocIDRsp)
	return rsp, nil
}

// GetLeader returns current leader
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

// IsClusterBootstrap returns cluster is bootstrap already
func (h *RPCHandler) IsClusterBootstrap(c context.Context, req *pb.IsClusterBootstrapReq) (*pb.IsClusterBootstrapRsp, error) {
	doFun := func() (interface{}, error) {
		return &pb.IsClusterBootstrapRsp{
			Value: h.server.isClusterBootstrapped(),
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.IsClusterBootstrapped(c, req)
	}

	tmp, err := h.doHandle("IsClusterBootstrap", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	rsp, _ := tmp.(*pb.IsClusterBootstrapRsp)
	return rsp, nil
}

// BootstrapCluster used for bootstrap cluster
func (h *RPCHandler) BootstrapCluster(c context.Context, req *pb.BootstrapClusterReq) (*pb.BootstrapClusterRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.bootstrapCluster(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.BootstrapCluster(c, req)
	}

	tmp, err := h.doHandle("BootstrapCluster", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	rsp, _ := tmp.(*pb.BootstrapClusterRsp)
	return rsp, nil
}

func (h *RPCHandler) doHandle(name string, req pb.BaseReq, forwardFun func(*pd.Client) (interface{}, error), doFun func() (interface{}, error)) (interface{}, error) {
	log.Debugf("rpc: get a req<%s-%d>, type=<%s> req=<%v>",
		req.GetFrom(),
		req.GetId(),
		name,
		req)

	// forward to leader
	if !h.server.IsLeader() {
		proxy := h.server.GetLeaderProxy()
		if nil == proxy {
			return nil, errNotLeader
		}

		log.Debugf("rpc: forward a req<%s-%d>, target=<%s>",
			req.GetFrom(),
			req.GetId(),
			proxy.GetLastPD())
		return forwardFun(proxy)
	}

	return doFun()
}
