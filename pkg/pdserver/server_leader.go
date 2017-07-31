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
	"sync/atomic"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
)

var (
	loopInterval = 200 * time.Millisecond
)

func (s *Server) startLeaderLoop() {
	s.leaderSignature = s.marshalLeader()

	for {
		if s.isClosed() {
			log.Infof("leader-loop: server is closed, return leader loop")
			return
		}

		leader, err := s.store.GetCurrentLeader()
		if err != nil {
			if !s.callStop {
				log.Errorf("leader-loop: get current leader failure, errors:\n %+v",
					err)
				time.Sleep(loopInterval)
				continue
			} else {
				return
			}
		}

		if leader != nil {
			if s.isMatchLeader(leader) {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can resign and campaign again.
				log.Warnf("leader-loop: leader is matched, resign and campaign again, leader=<%v>",
					leader)
				if err = s.store.ResignLeader(s.leaderSignature); err != nil {
					log.Warnf("leader-loop: resign leader failure, leader=<%v>, errors:\n %+v",
						leader,
						err)
					time.Sleep(loopInterval)
					continue
				}
			} else {
				log.Infof("leader-loop: we are not leader, watch the leader, leader=<%v>",
					leader)
				s.resetLeaderRPCProxy(leader)
				s.notifyElectionComplete()
				s.store.WatchLeader()
				log.Infof("leader-loop: leader changed, try to campaign leader, leader=<%v>", leader)
			}
		}

		log.Debugf("leader-loop: begin to campaign leader, name=<%s>",
			s.cfg.Name)
		if err = s.store.CampaignLeader(s.leaderSignature, s.cfg.LeaseSecsTTL, s.enableLeader); err != nil {
			if !s.callStop {
				log.Errorf("leader-loop: campaign leader failure, errors:\n %+v", err)
			}
		}
	}
}

func (s *Server) enableLeader() {
	s.notifyElectionComplete()

	// now, we are leader
	atomic.StoreInt64(&s.isLeaderValue, 1)
	log.Infof("leader-loop: PD cluster leader is ready, leader=<%s>", s.cfg.Name)

	// if we start cell cluster failure, exit pd.
	// Than other pd will become leader and try to start cell cluster
	err := s.cluster.start()
	if err != nil {
		log.Fatalf("leader-loop: start cell cluster failure, errors:\n %+v", err)
		return
	}
}

func (s *Server) disableLeader() {
	// now we are not leader
	atomic.StoreInt64(&s.isLeaderValue, 0)
}

func (s *Server) isMatchLeader(leader *pdpb.Leader) bool {
	return leader != nil &&
		s.cfg.RPCAddr == leader.GetAddr() &&
		s.id == leader.GetID()
}

// IsLeader returns whether server is leader or not.
func (s *Server) IsLeader() bool {
	return atomic.LoadInt64(&s.isLeaderValue) == 1
}

// GetLeader returns current leader pd for API
func (s *Server) GetLeader() (*pdpb.Leader, error) {
	return s.store.GetCurrentLeader()
}

func (s *Server) marshalLeader() string {
	leader := &pdpb.Leader{
		Addr:           s.cfg.RPCAddr,
		EctdClientAddr: s.cfg.EmbedEtcd.ClientUrls,
		ID:             s.id,
		Name:           s.cfg.Name,
	}

	return marshal(leader)
}

func marshal(leader *pdpb.Leader) string {
	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatalf("bootstrap: marshal leader failure, leader=<%v> errors:\n %+v",
			leader,
			err)
	}

	return string(data)
}
