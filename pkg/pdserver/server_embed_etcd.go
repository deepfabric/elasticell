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
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/types"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pdapi"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// unixToHTTP replace unix scheme with http.
var unixToHTTP = strings.NewReplacer("unix://", "http://", "unixs://", "http://")

var (
	maxCheckEtcdRunningCount = 60 * 10
	checkEtcdRunningDelay    = 1 * time.Second
)

func (s *Server) startEmbedEtcd() {
	log.Info("bootstrap: start embed etcd server")

	cfg, err := s.cfg.getEmbedEtcdConfig()
	if err != nil {
		log.Fatalf("bootstrap: start embed etcd server failure, errors:\n %+v",
			err)
		return
	}

	cfg.UserHandlers = map[string]http.Handler{
		fmt.Sprintf("%s/", pdapi.APIPrefix): pdapi.NewAPIHandler(s),
	}

	s.etcd, err = embed.StartEtcd(cfg)
	if err != nil {
		log.Fatalf("bootstrap: start embed etcd server failure, errors:\n %+v",
			err)
		return
	}

	select {
	case <-s.etcd.Server.ReadyNotify():
		log.Info("bootstrap: embed etcd server is ready")
		s.doAfterEmbedEtcdServerReady(cfg)
	case <-time.After(time.Minute):
		s.doStop()
	}
}

func (s *Server) doAfterEmbedEtcdServerReady(cfg *embed.Config) {
	s.checkEtcdCluster()

	s.id = uint64(s.etcd.Server.ID())
	log.Infof("bootstrap: embed server ids, id=<%d>, leader=<%d>",
		s.id,
		s.etcd.Server.Leader())

	s.initStore(cfg)
	s.updateAdvertisePeerUrls()

	if err := s.waitEtcdStart(cfg); err != nil {
		// See https://github.com/coreos/etcd/issues/6067
		// Here may return "not capable" error because we don't start
		// all etcds in initial_cluster at same time, so here just log
		// an error.
		// Note that pd can not work correctly if we don't start all etcds.
		log.Errorf("bootstrap: etcd start failed, err %v", err)
	}
}

func (s *Server) waitEtcdStart(cfg *embed.Config) error {
	var err error
	for i := 0; i < maxCheckEtcdRunningCount; i++ {
		// etcd may not start ok, we should wait and check again
		_, err = s.endpointStatus(cfg)
		if err == nil {
			return nil
		}

		time.Sleep(checkEtcdRunningDelay)
		continue
	}

	return err
}

// endpointStatus checks whether current etcd is running.
func (s *Server) endpointStatus(cfg *embed.Config) (*clientv3.StatusResponse, error) {
	c := s.store.RawClient()
	endpoint := []string{cfg.LCUrls[0].String()}[0]

	m := clientv3.NewMaintenance(c)

	start := time.Now()
	ctx, cancel := context.WithTimeout(c.Ctx(), DefaultRequestTimeout)
	resp, err := m.Status(ctx, endpoint)
	cancel()

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("bootstrap: check etcd status failed, endpoint=<%s> resp=<%+v> cost<%s> errors:\n %+v",
			endpoint,
			resp,
			cost,
			err)
	}

	return resp, errors.Wrapf(err, "")
}

func (s *Server) initStore(cfg *embed.Config) {
	store, err := NewStore(cfg)
	if err != nil {
		log.Fatalf("bootstrap: init store failure, cfg=<%v>, errors:\n %+v",
			cfg,
			err)
		return
	}

	s.store = store
}

func (s *Server) updateAdvertisePeerUrls() {
	members, err := s.store.GetCurrentClusterMembers()
	if err != nil {
		log.Fatalf("bootstrap: update current members of etcd cluster")
		return
	}

	for _, m := range members.Members {
		if s.id == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.EmbedEtcd.AdvertisePeerUrls != etcdPeerURLs {
				log.Infof("bootstrap: update advertise peer urls succ, old=<%s>, new=<%s>",
					s.cfg.EmbedEtcd.AdvertisePeerUrls,
					etcdPeerURLs)
				s.cfg.EmbedEtcd.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}
}

func (s *Server) checkEtcdCluster() {
	um, err := types.NewURLsMap(s.cfg.EmbedEtcd.InitialCluster)
	if err != nil {
		s.closeEmbedEtcd()
		log.Fatalf("bootstrap: check embed etcd server failure, errors:\n %+v",
			err)
		return
	}

	err = checkClusterID(s.etcd.Server.Cluster().ID(), um)
	if err != nil {
		log.Fatalf("bootstrap: check embed etcd server failure, errors:\n %+v",
			err)
		s.closeEmbedEtcd()
		return
	}
}

func (s *Server) closeEmbedEtcd() {
	if s.etcd == nil {
		return
	}

	if s.store != nil {
		s.store.Close()
		log.Info("stop: etcd v3 client is closed")
	}

	s.etcd.Close()
	log.Info("stop: embed etcd server is stopped")
}

func checkClusterID(localClusterID types.ID, um types.URLsMap) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for i, u := range peerURLs {
		u, gerr := url.Parse(u)
		if gerr != nil {
			return errors.Wrap(gerr, "check embed etcd")
		}

		trp := newHTTPTransport(u.Scheme)

		// For tests, change scheme to http.
		// etcdserver/api/v3rpc does not recognize unix protocol.
		if u.Scheme == "unix" || u.Scheme == "unixs" {
			peerURLs[i] = unixToHTTP.Replace(peerURLs[i])
		}

		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers([]string{peerURLs[i]}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Warnf("bootstrap: check etcd embed, may be member is not ready, member=<%s>",
				u)
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return errors.Wrapf(errEmbedEtcdClusterIDNotMatch,
				"expect=<%d>, got=<%d>",
				localClusterID,
				remoteClusterID)
		}
	}

	return nil
}

func newHTTPTransport(scheme string) *http.Transport {
	tr := &http.Transport{}
	if scheme == "unix" || scheme == "unixs" {
		tr.Dial = unixDial
	}
	return tr
}

func unixDial(_, addr string) (net.Conn, error) {
	return net.Dial("unix", addr)
}
