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
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/url"
	"strings"

	"github.com/coreos/etcd/embed"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

var (
	cfgFile = flag.String("cfg", "./pd.json", "Configuration file of pd server base on json formart.")
)

// Cfg pd server Cfg
type Cfg struct {
	Name    string `json:"name"`
	DataDir string `json:"dataDir"`

	// for embed etcd server
	ClientUrls          string `json:"clientUrls"`
	PeerUrls            string `json:"peerUrls"`
	AdvertiseClientUrls string `json:"advertiseClientUrls"`
	AdvertisePeerUrls   string `json:"advertisePeerUrls"`
	InitialCluster      string `json:"initialCluster"`
	InitialClusterState string `json:"initialClusterState"`

	// for leader election
	LeaseSecsTTL int64 `json:"leaseSecsTTL"`

	// RPCAddr rpc addr
	RPCAddr string `json:"rpcAddr"`

	LogLevel string `json:"logLevel, omitempty"`
	LogFile  string `json:"logFile, omitempty"`
}

// GetCfg get cfg from command
func GetCfg() *Cfg {
	data, err := ioutil.ReadFile(*cfgFile)
	if err != nil {
		log.Fatalf("bootstrap: read configuration file failure, cfg=<%s>, errors:\n %+v",
			*cfgFile,
			err)
		return nil
	}

	cfg, err := unmarshal(data)
	if err != nil {
		log.Fatalf("bootstrap: parse configuration file failure, cfg=<%s>, errors:\n %+v",
			*cfgFile,
			err)
		return nil
	}

	return cfg
}

func unmarshal(data []byte) (*Cfg, error) {
	v := &Cfg{}

	err := json.Unmarshal(data, v)

	if nil != err {
		return nil, errors.Wrap(err, "")
	}

	return v, nil
}

func (c *Cfg) getEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.EnablePprof = true

	var err error
	cfg.LPUrls, err = parseUrls(c.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = parseUrls(util.GetStringValue(c.AdvertisePeerUrls, c.PeerUrls))
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = parseUrls(c.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = parseUrls(util.GetStringValue(c.AdvertiseClientUrls, c.ClientUrls))
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Cfg) string() string {
	return ""
}

func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Wrap(err, "parse url error")
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
