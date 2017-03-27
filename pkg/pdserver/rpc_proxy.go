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
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
)

func (s *Server) resetLeaderRPCProxy(leader *pdpb.Leader) {
	s.leaderMux.Lock()
	defer s.leaderMux.Unlock()

	if s.leaderProxy != nil {
		err := s.leaderProxy.Close()
		if err != nil {
			log.Errorf("leader-proxy: close prev leader proxy failure, target=<%s> errors:\n %+v",
				s.leaderProxy.GetLastPD(),
				err)
		}

		log.Infof("leader-proxy: close prev leader proxy succ, target=<%s>",
			s.leaderProxy.GetLastPD())

		s.leaderProxy = nil
	}

	var err error
	s.leaderProxy, err = pd.NewClient(s.cfg.Name, leader.Addr)
	if err != nil {
		log.Errorf("leader-proxy: create leader proxy failure, target=<%v> errors:\n %+v",
			leader,
			err)
		return
	}

	log.Infof("leader-proxy: create leader proxy succ, target=<%v>",
		leader)
}

// GetLeaderProxy returns current leader proxy
func (s *Server) GetLeaderProxy() *pd.Client {
	s.leaderMux.RLock()
	defer s.leaderMux.RUnlock()
	return s.leaderProxy
}
