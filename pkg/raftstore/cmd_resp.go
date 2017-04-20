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

package raftstore

import (
	"errors"

	"github.com/deepfabric/elasticell/pkg/pb/errorpb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
)

var (
	errStaleCMD       = errors.New("stale command")
	errMissingUUIDCMD = errors.New("missing request uuid")

	infoStaleCMD = new(errorpb.StaleCommand)
)

func buildTerm(term uint64, resp *raftcmdpb.RaftCMDResponse) {
	resp.Header.CurrentTerm = term
}

func buildUUID(uuid []byte, resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header.UUID != nil {
		resp.Header.UUID = uuid
	}
}

func errorOtherCMDResp(err error) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(nil, 0)
	resp.Header.Error.Message = err.Error()
	return resp
}

func errorStaleCMDResp(uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)

	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleCommand = infoStaleCMD

	return resp
}

func errorStaleEpochResp(uuid []byte, currentTerm uint64, newCells ...metapb.Cell) *raftcmdpb.RaftCMDResponse {
	resp := errorBaseResp(uuid, currentTerm)

	resp.Header.Error.Message = errStaleCMD.Error()
	resp.Header.Error.StaleEpoch = &errorpb.StaleEpoch{
		NewCells: newCells,
	}

	return resp
}

func errorBaseResp(uuid []byte, currentTerm uint64) *raftcmdpb.RaftCMDResponse {
	resp := new(raftcmdpb.RaftCMDResponse)
	resp.Header = new(raftcmdpb.RaftResponseHeader)
	buildTerm(currentTerm, resp)
	buildUUID(uuid, resp)

	return resp
}
