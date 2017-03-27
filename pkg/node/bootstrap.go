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
	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	pb "github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"

	"golang.org/x/net/context"
)

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
		if n.store.Id > pd.ZeroID {
			log.Fatalf(`bootstrap: the cluster is not bootstrapped, 
			            but local store has a normal id<%d>, 
			            please check your configuration, 
			            maybe you are connect to a wrong pd server.`,
				n.store.Id)
			return
		}

		n.doBootstrapCluster()
	}

	// start heartbeat loop to driver anything
	n.startHeartbeat()
}

func (n *Node) doBootstrapCluster() {
	err := n.initStore()
	if err != nil {
		log.Fatalf("bootstrap: create store failure, errors:\n %+v", err)
		return
	}
	log.Infof("bootstrap: store created, store=<%v>", n.metaDB)

	cell, err := n.initFirstCell()
	if err != nil {
		log.Fatalf("bootstrap: create first cell failure, errors:\n %+v", err)
		return
	}
	log.Infof("bootstrap: first cell created, cell=<%v>", cell)

	req := &pb.BootstrapClusterReq{
		Store: n.store,
		Cell:  cell,
	}

	// If more than one node try to bootstrap the cluster at the same time,
	// Only one can succeed, others will get the `AlreadyBootstrapped` flag.
	// If we get any error, we will delete local cell
	rsp, err := n.pdClient.BootstrapCluster(context.TODO(), req)
	if err != nil {
		err := n.deleteCell(cell.Id)
		if err != nil {
			log.Errorf("bootstrap: cell delete failure, cell=<%d>, errors:\n %+v", cell.Id, err)
		}

		log.Fatalf("bootstrap: bootstrap cluster failure, req=<%v> errors:\n %+v",
			req,
			err)
		return
	} else if err == nil && rsp.GetAlreadyBootstrapped() {
		log.Info("bootstrap: the cluster is already bootstrapped")

		err := n.deleteCell(cell.Id)
		if err != nil {
			log.Fatalf("bootstrap: cell delete failure, cell=<%d>, errors:\n %+v", cell.Id, err)
		}
	}
}

func (n *Node) initStore() error {
	id, err := n.getAllocID()
	if err != nil {
		return err
	}

	m := n.newStore(id)
	err = n.metaDB.SaveStore(m)
	if err != nil {
		return err
	}

	n.store = m
	return nil
}

func (n *Node) initFirstCell() (meta.Cell, error) {
	cellID, err := n.getAllocID()
	if err != nil {
		return meta.Cell{}, err
	}

	peerID, err := n.getAllocID()
	if err != nil {
		return meta.Cell{}, err
	}

	m := meta.NewCell(cellID, peerID, n.store.Id)
	err = n.metaDB.SaveCell(m)
	if err != nil {
		return meta.Cell{}, err
	}

	n.cells[m.Id] = &m
	return m, nil
}
