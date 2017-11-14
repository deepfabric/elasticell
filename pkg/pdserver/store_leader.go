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
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	proto "github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// GetCurrentLeader return current leader
func (s *pdStore) GetCurrentLeader() (*pdpb.Leader, error) {
	resp, err := s.getValue(pdLeaderPath)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	if nil == resp {
		return nil, nil
	}

	v := &pdpb.Leader{}
	err = proto.Unmarshal(resp, v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

// ResignLeader delete leader itself and let others start a new election again.
func (s *pdStore) ResignLeader(leaderSignature string) error {
	resp, err := s.leaderTxn(leaderSignature).Then(clientv3.OpDelete(pdLeaderPath)).Commit()
	if err != nil {
		return errors.Wrap(err, "")
	}

	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

// WatchLeader watch leader,
// this funcation will return unitl the leader's lease is timeout
// or server closed
func (s *pdStore) WatchLeader() {
	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	ctx := s.client.Ctx()
	for {
		rch := watcher.Watch(ctx, pdLeaderPath)
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

// CampaignLeader is for leader election
// if we are win the leader election, the enableLeaderFun will call
func (s *pdStore) CampaignLeader(leaderSignature string, leaderLeaseTTL int64, enableLeaderFun func()) error {
	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, leaderLeaseTTL)
	cancel()

	if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
		log.Warnf("embed-etcd: lessor grants too slow, cost=<%s>", cost)
	}

	if err != nil {
		return errors.Wrap(err, "")
	}

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(pdLeaderPath), "=", 0)).
		Then(clientv3.OpPut(pdLeaderPath, leaderSignature, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return errors.Wrap(err, "")
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	// Make the leader keepalived.
	ch, err := lessor.KeepAlive(s.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return errors.Wrap(err, "")
	}

	enableLeaderFun()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("leader-loop: channel that keep alive for leader lease is closed")
				return nil
			}
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		}
	}
}

// txn returns an etcd client transaction wrapper.
// The wrapper will set a request timeout to the context and log slow transactions.
func (s *pdStore) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

func (s *pdStore) leaderTxn(leaderSignature string, cs ...clientv3.Cmp) clientv3.Txn {
	return newSlowLogTxn(s.client).If(append(cs, s.leaderCmp(leaderSignature))...)
}

func (s *pdStore) leaderCmp(leaderSignature string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(pdLeaderPath), "=", leaderSignature)
}
