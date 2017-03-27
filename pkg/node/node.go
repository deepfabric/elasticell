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
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// Node node
type Node struct {
	sync.RWMutex

	cfg *Cfg

	clusterID uint64

	pdClient *pd.Client
	metaDB   *storage.MetaDB

	// meta cache
	store meta.Store
	cells map[uint64]*meta.Cell

	storeHeartbeat *loop
	cellHeartbeats []*loop
}

// NewNode create a node instance, then init store, pd connection and init the cluster ID
func NewNode(cfg *Cfg, metaDB *storage.MetaDB) (*Node, error) {
	n := new(Node)
	n.cfg = cfg
	n.cells = make(map[uint64]*meta.Cell, 64)
	n.metaDB = metaDB

	err := n.initPDClient()
	if err != nil {
		return nil, err
	}

	err = n.loadMetaFromLocal()
	if err != nil {
		return nil, err
	}

	return n, nil
}

// Start start the node.
// if cluster is not bootstrapped, bootstrap cluster and create the first cell.
func (n *Node) Start() {
	n.bootstrapCluster()
}

// Stop the node
func (n *Node) Stop() error {
	n.stopHeartbeat()
	n.closePDClient()

	return nil
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
	c, err := pd.NewClient(n.cfg.StoreAddr, n.cfg.PDEndpoints...)
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

	n.clusterID = rsp.GetId()
	log.Infof("bootstrap: clusterID=<%d>", n.clusterID)

	return nil
}

func (n *Node) getAllocID() (uint64, error) {
	rsp, err := n.pdClient.AllocID(context.TODO(), new(pdpb.AllocIDReq))
	if err != nil {
		return pd.ZeroID, err
	}

	return rsp.GetId(), nil
}

func (n *Node) getCell(id uint64) meta.Cell {
	n.RLock()
	defer n.RUnlock()

	return *n.cells[id]
}

func (n *Node) loadMetaFromLocal() error {
	return nil
}
