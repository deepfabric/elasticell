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

package api

import (
	pb "github.com/deepfabric/elasticell/pkg/pdpb"
	"golang.org/x/net/context"
)

// RPCHandler it's a grpc interface implemention
type RPCHandler struct {
}

// NewRPCHandler create a new instance
func NewRPCHandler() pb.PDServiceServer {
	return &RPCHandler{}
}

// GetLeader get current leader
func (h *RPCHandler) GetLeader(context.Context, *pb.LeaderReq) (*pb.LeaderRsp, error) {
	return nil, nil
}
