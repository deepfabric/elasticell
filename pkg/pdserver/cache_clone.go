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
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/gogo/protobuf/proto"
)

func (s *StoreInfo) clone() *StoreInfo {
	v := new(StoreInfo)
	v.Meta = s.Meta

	if s.Status != nil {
		v.Status = new(StoreStatus)
		v.Status.Stats = new(pdpb.StoreStats)
		*v.Status.Stats = *s.Status.Stats
		v.Status.blocked = s.Status.blocked
		v.Status.LastHeartbeatTS = s.Status.LastHeartbeatTS
		v.Status.LeaderCount = s.Status.LeaderCount
	}

	return v
}

func (cc *CellInfo) clone() *CellInfo {
	c := &CellInfo{
		Meta: *(proto.Clone(&cc.Meta).(*metapb.Cell)),
	}

	if len(cc.DownPeers) > 0 {
		c.DownPeers = make([]pdpb.PeerStats, 0, len(cc.DownPeers))
		for _, peer := range cc.DownPeers {
			p := proto.Clone(&peer).(*pdpb.PeerStats)
			c.DownPeers = append(c.DownPeers, *p)
		}
	}

	if len(cc.PendingPeers) > 0 {
		c.PendingPeers = make([]metapb.Peer, 0, len(cc.PendingPeers))
		for _, peer := range cc.PendingPeers {
			p := proto.Clone(&peer).(*metapb.Peer)
			c.PendingPeers = append(c.PendingPeers, *p)
		}
	}

	if cc.LeaderPeer != nil {
		c.LeaderPeer = proto.Clone(cc.LeaderPeer).(*metapb.Peer)
	}

	return c
}
