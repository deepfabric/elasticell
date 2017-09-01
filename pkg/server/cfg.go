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

package server

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/deepfabric/elasticell/pkg/node"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

var (
	cfgFile = flag.String("cfg", "./cell.json", "Configuration file of cell kv server base on json formart.")
)

// Cfg server configuration
type Cfg struct {
	Redis  *RedisCfg       `json:"redis"`
	Node   *node.Cfg       `json:"node"`
	Metric *util.MetricCfg `json:"metric"`
}

// RedisCfg is used for configuration
type RedisCfg struct {
	Listen string `json:"listen"`

	ReadBufferSize  int   `json:"readBufferSize"`
	WriteBufferSize int   `json:"writeBufferSize"`
	WriteBatchLimit int64 `json:"writeBatchLimit"`
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
