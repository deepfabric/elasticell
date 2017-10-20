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

	"github.com/deepfabric/elasticell/pkg/node"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/pkg/errors"
)

// Cfg server configuration
type Cfg struct {
	AddrCli        string
	BufferCliRead  int
	BufferCliWrite int
	BatchCliResps  int64
	Node           *node.Cfg
	Metric         *util.MetricCfg
}

// NewCfg returns default cfg
func NewCfg() *Cfg {
	return &Cfg{
		Node: node.NewCfg(),
	}
}

func unmarshal(data []byte) (*Cfg, error) {
	v := &Cfg{}

	err := json.Unmarshal(data, v)

	if nil != err {
		return nil, errors.Wrap(err, "")
	}

	return v, nil
}
