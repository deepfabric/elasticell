package node

import (
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/storage"
)

// Node node
type Node struct {
	cfg *Cfg

	clusterID int64
	nodeID    int64
	pdClient  *pd.Client
	store     *storage.Store
}

// NewNode create a node instance, then init store, pd connection and init the cluster ID
func NewNode(cfg *Cfg) (*Node, error) {
	n := new(Node)

	err := n.initPDClient()
	if err != nil {
		return nil, err
	}

	err = n.initStore()
	if err != nil {
		return nil, err
	}

	n.fetchClusterIDFromStore()
	n.fetchNodeIDFromStore()

	return n, nil
}

// Start start the node.
// if cluster is not bootstrapped, bootstrap cluster and create the first cell.
func (n *Node) Start() {
	n.bootstrapCluster()
}

// Stop the node
func (n *Node) Stop() error {
	return nil
}

func (n *Node) bootstrapCluster() {
	id, err := n.pdClient.GetClusterID()
	if err != nil {
		log.Fatalf("bootstrap: get cluster id from pd failure, pd=<%s>, errors:\n %+v",
			n.cfg.PDEndpoints,
			err)
		return
	}

	n.clusterID = id
	log.Infof("bootstrap: clusterID=<%d>", id)

	ok, err := n.pdClient.IsClusterBootstrapped()
	if err != nil {
		log.Fatalf("bootstrap: check cluster bootstrap status failure,  errors:\n %+v", err)
		return
	}

	// If cluster is not bootstrapped, we will bootstrap the cluster, and create the first cell
	if !ok {
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
}

func (n *Node) initStore() error {
	return nil
}

func (n *Node) initPDClient() error {
	return nil
}

func (n *Node) fetchClusterIDFromStore() {

}

func (n *Node) fetchNodeIDFromStore() {

}

func (n *Node) doBootstrapCluster() {
	cell, err := n.createFirstCell()
	if err != nil {
		log.Fatalf("bootstrap: create first cell filaure, errors:\n %+v", err)
		return
	}

	log.Infof("bootstrap: first cell created, cell=<%s>", cell.String())

	// If more than one node try to bootstrap the cluster at the same time,
	// Only one can succeed, others will get the `ErrClusterIsAlreadyBootstrapped` error.
	// If we get any error, we will delete locale cell
	err = n.pdClient.BootstrapCluster(cell)
	if err != nil && err != pd.ErrClusterIsAlreadyBootstrapped {
		log.Fatalf("bootstrap: bootstrap cluster failure, errors:\n %+v", err)
		n.rollbackFirstCell(cell.CellID)
		return
	} else if err != nil && err == pd.ErrClusterIsAlreadyBootstrapped {
		log.Info("bootstrap: the cluster is already bootstrapped")
		n.rollbackFirstCell(cell.CellID)
	}

	// That's ok, the cluster is bootstrapped succ.
	// Now we can start store, and report info to pd.
	err = n.startStoreAndTellToPD()
	if err != nil {
		log.Fatalf("bootstrap: start local store and tell to pd server failure, errors:\n %+v", err)
		return
	}

	// ok, left thing driverd by pd.
}

func (n *Node) createFirstCell() (*storage.Cell, error) {
	id, err := n.pdClient.AllocID()
	if err != nil {
		return nil, err
	}

	return storage.NewCell(id), nil
}

func (n *Node) rollbackFirstCell(id int64) {
	err := n.store.DeleteLocalCell(id)
	if err != nil {
		log.Errorf("bootstrap: rollback cell failure, cell=<%d>, errors:\n %+v", id, err)
		return
	}

	log.Infof("bootstrap: cell rollback succ, cell=<%d>", id)
}

func (n *Node) startStoreAndTellToPD() error {
	err := n.store.Start()
	if err != nil {
		return err
	}

	err = n.pdClient.TellPDStoreStarted(n.store)
	if err != nil {
		return err
	}

	return nil
}
