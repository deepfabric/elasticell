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

package node

import (
	"encoding/json"
	"sync"

	"github.com/deepfabric/elasticell/pkg/pdapi"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/raftstore"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/deepfabric/elasticell/pkg/util"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// Node node
type Node struct {
	sync.RWMutex

	cfg       *Cfg
	clusterID uint64
	pdClient  *pd.Client
	driver    storage.Driver
	storeMeta metapb.Store
	store     *raftstore.Store

	runner *util.Runner
}

// NewNode create a node instance, then init store, pd connection and init the cluster ID
func NewNode(clientAddr string, cfg *Cfg, driver storage.Driver) (*Node, error) {
	n := new(Node)
	n.cfg = cfg
	n.driver = driver
	n.clusterID = cfg.ClusterID
	n.storeMeta = newStore(clientAddr, cfg)
	n.runner = util.NewRunner()

	err := n.initPDClient()
	if err != nil {
		return nil, err
	}

	return n, nil
}

// Start start the node.
// if cluster is not bootstrapped, bootstrap cluster and create the first cell.
func (n *Node) Start() *raftstore.Store {
	bootstrapped := n.checkClusterBootstrapped()
	storeID := n.checkStore()

	if storeID == pd.ZeroID {
		storeID = n.bootstrapStore()
	} else if !bootstrapped {
		log.Fatalf(`bootstrap: store is not empty, but the cluster is not bootstrapped,
					maybe you connected a wrong PD or need to remove the data and start again. 
					storeID=<%d> clusterID=<%d>`,
			storeID,
			n.clusterID)
	}

	n.storeMeta.ID = storeID

	if !bootstrapped {
		cells := n.bootstrapCells()
		n.bootstrapCluster(cells)
	}

	n.startStore()
	n.putStore()

	return n.store
}

// Stop the node
func (n *Node) Stop() error {
	err := n.runner.Stop()
	n.closePDClient()

	return err
}

func (n *Node) closePDClient() {
	if n.pdClient != nil {
		err := n.pdClient.Close()
		if err != nil {
			log.Errorf("stop: stop pd client failure, errors:\n %+v", err)
			return
		}
	}

	log.Info("stop: pd client stopped")
}

func (n *Node) initPDClient() error {
	c, err := pd.NewClient(n.cfg.RaftStore.StoreAddr, n.cfg.PDEndpoints...)
	if err != nil {
		return errors.Wrap(err, "")
	}

	n.pdClient = c
	rsp, err := n.pdClient.GetClusterID(context.TODO(), new(pdpb.GetClusterIDReq))
	if err != nil {
		log.Fatalf("bootstrap: get cluster id from pd failure, pd=<%s>, errors:\n %+v",
			n.cfg.PDEndpoints,
			err)
		return errors.Wrap(err, "")
	}

	n.clusterID = rsp.GetID()
	log.Infof("bootstrap: clusterID=<%d>", n.clusterID)

	return nil
}

func (n *Node) getAllocID() (uint64, error) {
	rsp, err := n.pdClient.AllocID(context.TODO(), new(pdpb.AllocIDReq))
	if err != nil {
		return pd.ZeroID, err
	}

	return rsp.GetID(), nil
}

func (n *Node) getInitParam() (*pdapi.InitParams, error) {
	rsp, err := n.pdClient.GetInitParams(context.TODO(), new(pdpb.GetInitParamsReq))
	if err != nil {
		return nil, err
	}

	params := &pdapi.InitParams{
		InitCellCount: 1,
	}

	if len(rsp.Params) > 0 {
		err = json.Unmarshal(rsp.Params, params)
		if err != nil {
			return nil, err
		}
	}

	return params, nil
}

func newStore(clientAddr string, cfg *Cfg) metapb.Store {
	addr := cfg.RaftStore.StoreAddr
	if cfg.RaftStore.StoreAdvertiseAddr != "" {
		addr = cfg.RaftStore.StoreAdvertiseAddr
	}

	return metapb.Store{
		Address:       addr,
		ClientAddress: clientAddr,
		Lables:        cfg.StoreLables,
		State:         metapb.UP,
	}
}
