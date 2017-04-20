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
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
)

type raftCMD struct {
	sentTime time.Time
	req      *raftcmdpb.RaftCMDRequest
	callback func(*raftcmdpb.RaftCMDResponse)
}

func (cmd *raftCMD) resp(rsp *raftcmdpb.RaftCMDResponse) {
	if cmd.callback != nil {
		cmd.callback(rsp)
	}
}

func newRaftCmd(req *raftcmdpb.RaftCMDRequest, callback func(*raftcmdpb.RaftCMDResponse)) *raftCMD {
	return &raftCMD{
		sentTime: time.Now(),
		req:      req,
		callback: callback,
	}
}
