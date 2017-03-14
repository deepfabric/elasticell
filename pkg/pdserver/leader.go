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
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	pb "github.com/deepfabric/elasticell/pkg/pdpb"
)

var (
	loopInterval = 200 * time.Millisecond
)

func (s *Server) startLeaderLoop() {
	for {
		if s.isClosed() {
			log.Infof("leader-loop: server is closed, return leader loop")
			return
		}

		leader, err := s.store.GetCurrentLeader()
		if err != nil {
			log.Errorf("leader-loop: get current leader failure, errors:\n %+v",
				err)
			time.Sleep(loopInterval)
			continue
		}

		if leader != nil {
			if s.isMatchLeader(leader) {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can resign and campaign again.
				log.Warnf("leader-loop: leader is matched, resign and campaign again, leader=<%s>",
					leader)
				if err = s.store.ResignLeader(s.leaderSignature); err != nil {
					log.Warnf("leader-loop: resign leader failure, leader=<%s>, errors:\n %+v",
						leader,
						err)
					time.Sleep(loopInterval)
					continue
				}
			} else {
				log.Infof("leader-loop: leader is not matched, watch it, leader=<%s>",
					leader)
				s.store.WatchLeader()
				log.Info("leader-loop: leader changed, try to campaign leader, leader=<%s>", leader)
			}
		}

		log.Debugf("leader-loop: begin to campaign leader, name=<%s> leader=<%s>",
			s.cfg.Name,
			s.leaderSignature)
		if err = s.store.CampaignLeader(s.leaderSignature, s.cfg.LeaseTTL); err != nil {
			log.Errorf("leader-loop: campaign leader failure, errors:\n %+v", err)
		}
	}
}

func (s *Server) isMatchLeader(leader *pb.Leader) bool {
	return leader != nil &&
		s.cfg.RPCAddr == leader.GetAddr() &&
		s.id == leader.GetId()
}

func (s *Server) marshalLeader() string {
	leader := &pb.Leader{
		Addr: s.cfg.RPCAddr,
		Id:   s.id,
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatalf("bootstrap: marshal leader failure, leader=<%v> errors:\n %+v",
			leader,
			err)
	}

	return string(data)
}
