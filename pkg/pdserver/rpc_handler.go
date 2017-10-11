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
	"github.com/deepfabric/elasticell/pkg/pb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
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
func NewRPCHandler(server *Server) pdpb.PDServiceServer {
	return &RPCHandler{
		server: server,
	}
}

// GetClusterID returns cluster id
func (h *RPCHandler) GetClusterID(c context.Context, req *pdpb.GetClusterIDReq) (*pdpb.GetClusterIDRsp, error) {
	doFun := func() (interface{}, error) {
		return &pdpb.GetClusterIDRsp{
			ID: h.server.GetClusterID(),
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.GetClusterID(c, req)
	}

	rsp, err := h.doHandle("GetClusterID", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.GetClusterIDRsp), nil
}

// GetInitParams returns cluster init params
func (h *RPCHandler) GetInitParams(c context.Context, req *pdpb.GetInitParamsReq) (*pdpb.GetInitParamsRsp, error) {
	doFun := func() (interface{}, error) {
		params, err := h.server.GetInitParamsValue()
		if err != nil {
			return nil, err
		}

		return &pdpb.GetInitParamsRsp{
			Params: params,
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.GetInitParams(c, req)
	}

	rsp, err := h.doHandle("GetInitParams", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.GetInitParamsRsp), nil
}

// AllocID returns alloc id for kv node
func (h *RPCHandler) AllocID(c context.Context, req *pdpb.AllocIDReq) (*pdpb.AllocIDRsp, error) {
	doFun := func() (interface{}, error) {
		id, err := h.server.idAlloc.newID()
		if err != nil {
			return nil, err
		}

		return &pdpb.AllocIDRsp{
			ID: id,
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.AllocID(c, req)
	}

	rsp, err := h.doHandle("AllocID", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.AllocIDRsp), nil
}

// GetLeader returns current leader
func (h *RPCHandler) GetLeader(c context.Context, req *pdpb.LeaderReq) (*pdpb.LeaderRsp, error) {
	doFun := func() (interface{}, error) {
		leader, err := h.server.store.GetCurrentLeader()
		if err != nil {
			return nil, err
		}

		return &pdpb.LeaderRsp{
			Leader: *leader,
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.GetLeader(c, req)
	}

	rsp, err := h.doHandle("GetLeader", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.LeaderRsp), nil
}

// IsClusterBootstrap returns cluster is bootstrap already
func (h *RPCHandler) IsClusterBootstrap(c context.Context, req *pdpb.IsClusterBootstrapReq) (*pdpb.IsClusterBootstrapRsp, error) {
	doFun := func() (interface{}, error) {
		return &pdpb.IsClusterBootstrapRsp{
			Value: h.server.isClusterBootstrapped(),
		}, nil
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.IsClusterBootstrapped(c, req)
	}

	rsp, err := h.doHandle("IsClusterBootstrap", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.IsClusterBootstrapRsp), nil
}

// BootstrapCluster returns bootstrap cluster response
func (h *RPCHandler) BootstrapCluster(c context.Context, req *pdpb.BootstrapClusterReq) (*pdpb.BootstrapClusterRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.bootstrapCluster(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.BootstrapCluster(c, req)
	}

	rsp, err := h.doHandle("BootstrapCluster", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.BootstrapClusterRsp), nil
}

// PutStore puts store
func (h *RPCHandler) PutStore(c context.Context, req *pdpb.PutStoreReq) (*pdpb.PutStoreRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.putStore(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.PutStore(c, req)
	}

	rsp, err := h.doHandle("PutStore", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.PutStoreRsp), nil
}

// GetStore get store info
func (h *RPCHandler) GetStore(c context.Context, req *pdpb.GetStoreReq) (*pdpb.GetStoreRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.getStore(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.GetStore(c, req)
	}

	rsp, err := h.doHandle("GetStore", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.GetStoreRsp), nil
}

// CellHeartbeat returns cell heartbeat response
func (h *RPCHandler) CellHeartbeat(c context.Context, req *pdpb.CellHeartbeatReq) (*pdpb.CellHeartbeatRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.cellHeartbeat(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.CellHeartbeat(c, req)
	}

	rsp, err := h.doHandle("CellHeartbeat", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.CellHeartbeatRsp), nil
}

// StoreHeartbeat returns store heartbeat response
func (h *RPCHandler) StoreHeartbeat(c context.Context, req *pdpb.StoreHeartbeatReq) (*pdpb.StoreHeartbeatRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.storeHeartbeat(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.StoreHeartbeat(c, req)
	}

	rsp, err := h.doHandle("StoreHeartbeat", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.StoreHeartbeatRsp), nil
}

// AskSplit returns ask split response
func (h *RPCHandler) AskSplit(c context.Context, req *pdpb.AskSplitReq) (*pdpb.AskSplitRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.askSplit(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.AskSplit(c, req)
	}

	rsp, err := h.doHandle("AskSplit", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.AskSplitRsp), nil
}

// ReportSplit returns report split response
func (h *RPCHandler) ReportSplit(c context.Context, req *pdpb.ReportSplitReq) (*pdpb.ReportSplitRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.reportSplit(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.ReportSplit(c, req)
	}

	rsp, err := h.doHandle("AskSplit", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.ReportSplitRsp), nil
}

// GetLastRanges returns lastest key ranges
func (h *RPCHandler) GetLastRanges(c context.Context, req *pdpb.GetLastRangesReq) (*pdpb.GetLastRangesRsp, error) {
	doFun := func() (interface{}, error) {
		return h.server.getLastRanges(req)
	}

	forwardFun := func(proxy *pd.Client) (interface{}, error) {
		return proxy.GetLastRanges(c, req)
	}

	rsp, err := h.doHandle("GetLastRanges", req, forwardFun, doFun)
	if err != nil {
		return nil, err
	}

	return rsp.(*pdpb.GetLastRangesRsp), nil
}

func (h *RPCHandler) doHandle(name string, req pb.BaseReq, forwardFun func(*pd.Client) (interface{}, error), doFun func() (interface{}, error)) (interface{}, error) {
	log.Debugf("rpc: req<%s-%d>, type=<%s> req=<%v>",
		req.GetFrom(),
		req.GetID(),
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
			req.GetID(),
			proxy.GetLastPD())
		return forwardFun(proxy)
	}

	rsp, err := doFun()
	if err == nil {
		log.Debugf("rpc: rsp<%s-%d>, rsp=<%v>",
			req.GetFrom(),
			req.GetID(),
			rsp,
		)
	}

	return rsp, err
}
