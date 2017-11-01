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
	"math"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/raftstore"
	"github.com/deepfabric/elasticell/pkg/util"
	"golang.org/x/net/context"
)

func (n *Node) checkClusterBootstrapped() bool {
	for index := 0; index < 60; index++ {
		rsp, err := n.pdClient.IsClusterBootstrapped(context.TODO(), new(pdpb.IsClusterBootstrapReq))
		if err != nil {
			log.Warnf("bootstrap: check cluster bootstrap status failure,  errors:\n %+v", err)
			time.Sleep(time.Second * 3)
		} else {
			return rsp.GetValue()
		}
	}

	log.Fatal("bootstrap: check cluster bootstrap failed")
	return false
}

func (n *Node) checkStore() uint64 {
	data, err := n.driver.GetEngine().Get(raftstore.GetStoreIdentKey())
	if err != nil {
		log.Fatalf("bootstrap: check store failed, errors:\n %+v", err)
	}

	if len(data) == 0 {
		return pd.ZeroID
	}

	st := new(mraft.StoreIdent)
	util.MustUnmarshal(st, data)

	if st.ClusterID != n.clusterID {
		log.Fatalf("bootstrap: check store failed, cluster id mismatch, local=<%d> remote=<%d>",
			st.ClusterID,
			n.clusterID)
	}

	if st.StoreID == pd.ZeroID {
		log.Fatal("bootstrap: check store failed, zero store id")
	}

	return st.StoreID
}

func (n *Node) bootstrapStore() uint64 {
	storeID, err := n.getAllocID()
	if err != nil {
		log.Fatalf("bootstrap: bootstrap store failed, errors:\n %+v", err)
		return pd.ZeroID
	}

	log.Infof("bootstrap: alloc store id succ, id=<%d>", storeID)

	count := 0
	err = n.driver.GetEngine().Scan(raftstore.GetMinKey(), raftstore.GetMaxKey(), func([]byte, []byte) (bool, error) {
		count++
		return false, nil
	}, false)

	if err != nil {
		log.Fatalf("bootstrap: bootstrap store failed, errors:\n %+v", err)
	}

	if count > 0 {
		log.Fatal("bootstrap: store is not empty and has already had data")
	}

	st := new(mraft.StoreIdent)
	st.ClusterID = n.clusterID
	st.StoreID = storeID

	err = n.driver.GetEngine().Set(raftstore.GetStoreIdentKey(), util.MustMarshal(st))
	if err != nil {
		log.Fatalf("bootstrap: bootstrap store failed, errors:\n %v", err)
	}

	return storeID
}

func (n *Node) bootstrapCells() []metapb.Cell {
	params, err := n.getInitParam()
	if err != nil {
		log.Fatalf("bootstrap: bootstrap cells failed, errors:\n %+v", err)
	}

	if params.InitCellCount < 1 {
		log.Fatalf("bootstrap: bootstrap cells failed, error bootstrap cell count: %d",
			params.InitCellCount)
	}

	var cells []metapb.Cell
	if params.InitCellCount == 1 {
		cells = append(cells, n.createCell(nil, nil))
		return cells
	}

	step := uint64(math.MaxUint64 / params.InitCellCount)
	start := uint64(0)
	end := start + step

	for i := uint64(0); i < params.InitCellCount; i++ {
		if params.InitCellCount-i == 1 {
			end = math.MaxUint64
		}

		cells = append(cells, n.createCell(util.Uint64ToBytes(start), util.Uint64ToBytes(end)))
		start = end
		end = start + step
	}

	return cells
}

func (n *Node) createCell(start, end []byte) metapb.Cell {
	cellID, err := n.getAllocID()
	if err != nil {
		log.Fatalf("bootstrap: bootstrap cells failed, errors:\n %+v", err)
	}

	log.Infof("bootstrap: alloc id for cells succ, id=<%d>", cellID)

	peerID, err := n.getAllocID()
	if err != nil {
		log.Fatalf("bootstrap: bootstrap first cell failed, errors:\n %+v", err)
	}

	log.Infof("bootstrap: alloc peer id for first cell succ, peerID=<%d> cellID=<%d>",
		peerID,
		cellID)

	cell := pb.NewCell(cellID, peerID, n.storeMeta.ID)
	cell.Start = start
	cell.End = end

	err = raftstore.SaveCell(n.driver, cell)
	if err != nil {
		log.Fatalf("bootstrap: bootstrap first cell failed, errors:\n %+v", err)
	}

	log.Infof("bootstrap: save first cell complete")

	return cell
}

func (n *Node) bootstrapCluster(cells []metapb.Cell) {
	req := &pdpb.BootstrapClusterReq{
		Store: n.storeMeta,
		Cells: cells,
	}

	// If more than one node try to bootstrap the cluster at the same time,
	// Only one can succeed, others will get the `AlreadyBootstrapped` flag.
	// If we get any error, we will delete local cells
	rsp, err := n.pdClient.BootstrapCluster(context.TODO(), req)
	if err != nil {
		if err != nil {
			log.Errorf("bootstrap: cell delete failure, errors:\n %+v",
				err)
		}

		log.Fatalf("bootstrap: bootstrap cluster failure, req=<%v> errors:\n %+v",
			req,
			err)
	} else if err == nil && rsp.GetAlreadyBootstrapped() {
		log.Info("bootstrap: the cluster is already bootstrapped")
	}
}

func (n *Node) startStore() {
	log.Infof("bootstrap: begin to start store, storeID=<%d>", n.storeMeta.ID)
	n.store = raftstore.NewStore(n.clusterID, n.pdClient, n.storeMeta, n.driver, n.cfg.RaftStore)

	params, err := n.getInitParam()
	if err != nil {
		log.Fatalf("bootstrap: bootstrap cells failed, errors:\n %+v", err)
	}

	if params.InitCellCount > 1 {
		n.store.SetKeyConvertFun(util.Uint64Convert)
	}

	n.runner.RunCancelableTask(func(c context.Context) {
		n.store.Start()
		<-c.Done()
		n.store.Stop()
		log.Infof("stopped: store stopped, storeID=<%d>", n.storeMeta.ID)
	})
}

func (n *Node) putStore() {
	req := new(pdpb.PutStoreReq)
	req.Header.ClusterID = n.clusterID
	req.Store = n.storeMeta
	_, err := n.pdClient.PutStore(context.TODO(), req)
	if err != nil {
		log.Fatalf("bootstrap: put store to pd failed, errors:\n %+v", err)
	}
}
