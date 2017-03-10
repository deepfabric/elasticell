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
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/types"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pdserver/storage"
	"github.com/pkg/errors"
)

// unixToHTTP replace unix scheme with http.
var unixToHTTP = strings.NewReplacer("unix://", "http://", "unixs://", "http://")

func (s *Server) startEmbedEtcd() {
	log.Info("bootstrap: start embed ectd server")

	cfg, err := s.cfg.getEmbedEtcdConfig()
	if err != nil {
		log.Fatalf("bootstrap: start embed ectd server failure, errors:\n %+v",
			err)
		return
	}

	s.etcd, err = embed.StartEtcd(cfg)
	if err != nil {
		log.Fatalf("bootstrap: start embed ectd server failure, errors:\n %+v",
			err)
		return
	}

	select {
	case <-s.etcd.Server.ReadyNotify():
		log.Info("bootstrap: embed etcd server is ready")
		s.doAfterEmbedEtcdServerReady(cfg)
	case <-s.stopC:
		s.doStop()
	}
}

func (s *Server) doAfterEmbedEtcdServerReady(cfg *embed.Config) {
	// Now, we need do some check for cluster
	s.checkEctdCluster()

	s.id = uint64(s.etcd.Server.ID())

	s.initStore(cfg)
	s.updateAdvertisePeerUrls()
}

func (s *Server) initStore(cfg *embed.Config) {
	store, err := storage.NewStore(cfg)
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
		log.Fatalf("bootstrap: update current members of ectd cluster")
		return
	}

	for _, m := range members.Members {
		if s.id == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Infof("bootstrap: update advertise peer urls succ, old=<%s>, new=<%s>", s.cfg.AdvertisePeerUrls, etcdPeerURLs)
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}
}

func (s *Server) checkEctdCluster() {
	um, err := types.NewURLsMap(s.cfg.InitialCluster)
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

	s.etcd.Close()
	log.Info("stop: embed ectd server is stopped")
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
			log.Warnf("bootstrap: check ectd embed, may be member is not ready, member=<%s>",
				u)
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return errors.Wrapf(ErrEmbedEctdClusterIDNotMatch,
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
