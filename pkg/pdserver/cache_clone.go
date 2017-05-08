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
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

func (s *storeRuntimeInfo) clone() *storeRuntimeInfo {
	v := new(storeRuntimeInfo)
	v.store = s.store

	if s.status != nil {
		v.status = new(StoreStatus)
		v.status.stats = new(pdpb.StoreStats)
		*v.status.stats = *s.status.stats
		v.status.blocked = s.status.blocked
		v.status.LastHeartbeatTS = s.status.LastHeartbeatTS
		v.status.LeaderCount = s.status.LeaderCount
	}

	return v
}
