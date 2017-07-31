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
	"github.com/coreos/etcd/embed"
	"github.com/deepfabric/elasticell/pkg/util"
)

// EmbedEtcdCfg is the embed etcd cfg
type EmbedEtcdCfg struct {
	ClientUrls          string `json:"clientUrls"`
	PeerUrls            string `json:"peerUrls"`
	AdvertiseClientUrls string `json:"advertiseClientUrls"`
	AdvertisePeerUrls   string `json:"advertisePeerUrls"`
	InitialCluster      string `json:"initialCluster"`
	InitialClusterState string `json:"initialClusterState"`
}

func (c *Cfg) getEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.EmbedEtcd.InitialCluster
	cfg.ClusterState = c.EmbedEtcd.InitialClusterState
	cfg.EnablePprof = false
	cfg.Debug = false

	var err error
	cfg.LPUrls, err = util.ParseUrls(c.EmbedEtcd.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = util.ParseUrls(util.GetStringValue(c.EmbedEtcd.AdvertisePeerUrls, c.EmbedEtcd.PeerUrls))
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = util.ParseUrls(c.EmbedEtcd.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = util.ParseUrls(util.GetStringValue(c.EmbedEtcd.AdvertiseClientUrls, c.EmbedEtcd.ClientUrls))
	if err != nil {
		return nil, err
	}

	return cfg, nil
}
