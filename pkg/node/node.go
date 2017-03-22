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
	"fmt"

	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pd"
	pb "github.com/deepfabric/elasticell/pkg/pdpb"
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
	store    *storage.Store
	cells    map[uint64]*storage.Cell

	storeHeartbeat *loop
	cellHeartbeats []*loop
}

// NewNode create a node instance, then init store, pd connection and init the cluster ID
func NewNode(cfg *Cfg, store *storage.Store) (*Node, error) {
	n := new(Node)
	n.cfg = cfg
	n.cells = make(map[uint64]*storage.Cell, 64)
	n.store = store

	err := n.initPDClient()
	if err != nil {
		return nil, err
	}

	err = n.loadLocalMeta()
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

	log.Info("stop: stop pd client succ")
}

func (n *Node) initPDClient() error {
	c, err := pd.NewClient("", n.cfg.PDEndpoints...)
	if err != nil {
		return errors.Wrap(err, "")
	}

	n.pdClient = c
	rsp, err := n.pdClient.GetClusterID(context.TODO(), new(pb.GetClusterIDReq))
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

func (n *Node) bootstrapCluster() {
	rsp, err := n.pdClient.IsClusterBootstrapped(context.TODO(), new(pb.IsClusterBootstrapReq))
	if err != nil {
		log.Fatalf("bootstrap: check cluster bootstrap status failure,  errors:\n %+v", err)
		return
	}

	// If cluster is not bootstrapped, we will bootstrap the cluster, and create the first cell
	if !rsp.GetValue() {
		// The cluster is not bootstrap, but current node has a normal store id.
		// So there has some error.
		if n.store.GetStoreID() > pd.ZeroID {
			log.Fatalf(`bootstrap: the cluster is not bootstrapped, 
			            but local store has a normal id<%d>, 
			            please check your configuration, 
			            maybe you are connect to a wrong pd server.`,
				n.store.GetStoreID())
			return
		}

		n.doBootstrapCluster()
	}

	// we use heartbeat loop to driver left things
	n.startHeartbeat()
}

func (n *Node) doBootstrapCluster() {
	err := n.createStore()
	if err != nil {
		log.Fatalf("bootstrap: create store failure, errors:\n %+v", err)
		return
	}

	n.storeHeartbeat = newLoop(fmt.Sprintf("store-%d", n.store.GetStoreID()),
		n.cfg.getStoreHeartbeatDuration(),
		n.doStoreHeartbeat,
		n.store.GetStoreID())
	log.Infof("bootstrap: store created, store=<%v>", n.store)

	cell, err := n.createFirstCell()
	if err != nil {
		log.Fatalf("bootstrap: create first cell failure, errors:\n %+v", err)
		return
	}

	n.cellHeartbeats = append(n.cellHeartbeats, newLoop(fmt.Sprintf("cell-%d", cell.GetID()),
		n.cfg.getCellHeartbeatDuration(),
		n.doCellHeartbeat,
		cell.GetID()))
	log.Infof("bootstrap: first cell created, cell=<%v>", cell)

	sm, err := n.store.GetStoreMeta()
	if err != nil {
		log.Fatalf("bootstrap: get store meta data failure, errors:\n %+v", err)
		return
	}

	cm, err := cell.GetCellMeta()
	if err != nil {
		log.Fatalf("bootstrap: get cell meta data failure, errors:\n %+v", err)
		return
	}
	n.cells[cell.GetID()] = cell

	req := &pb.BootstrapClusterReq{
		Store: pb.Meta{
			Data: sm,
		},
		Cell: pb.Meta{
			Data: cm,
		},
	}

	// If more than one node try to bootstrap the cluster at the same time,
	// Only one can succeed, others will get the `AlreadyBootstrapped` flag.
	// If we get any error, we will delete local cell
	rsp, err := n.pdClient.BootstrapCluster(context.TODO(), req)
	if err != nil {
		log.Fatalf("bootstrap: bootstrap cluster failure, req=<%v> errors:\n %+v",
			req,
			err)
		n.rollbackFirstCell(cell.GetID())
		return
	} else if err == nil && rsp.GetAlreadyBootstrapped() {
		log.Info("bootstrap: the cluster is already bootstrapped")
		n.rollbackFirstCell(cell.GetID())
	}
}

func (n *Node) createStore() error {
	id, err := n.getAllocID()
	if err != nil {
		return err
	}

	n.store.InitStoreInfo(id, n.cfg.StoreAddr, n.cfg.StoreLables)

	return n.store.Save()
}

func (n *Node) createFirstCell() (*storage.Cell, error) {
	id, err := n.getAllocID()
	if err != nil {
		return nil, err
	}

	return storage.NewCell(id, n.store.GetStoreID()), nil
}

func (n *Node) rollbackFirstCell(id uint64) {
	err := n.store.DeleteLocalCell(id)
	if err != nil {
		log.Errorf("bootstrap: rollback cell failure, cell=<%d>, errors:\n %+v", id, err)
		return
	}

	log.Infof("bootstrap: cell rollback succ, cell=<%d>", id)
}

func (n *Node) getAllocID() (uint64, error) {
	rsp, err := n.pdClient.AllocID(context.TODO(), new(pb.AllocIDReq))
	if err != nil {
		return pd.ZeroID, err
	}

	return rsp.GetId(), nil
}

func (n *Node) getCell(id uint64) *storage.Cell {
	n.RLock()
	defer n.RUnlock()

	return n.cells[id]
}
