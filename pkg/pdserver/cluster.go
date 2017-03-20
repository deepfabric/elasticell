package pdserver

import (
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	pb "github.com/deepfabric/elasticell/pkg/pdpb"
	"github.com/deepfabric/elasticell/pkg/storage/meta"
	"github.com/pkg/errors"
)

// GetCellCluster returns current cell cluster
// if not bootstrap, return nil
func (s *Server) GetCellCluster() *CellCluster {
	if s.isClosed() || !s.cluster.isRunning() {
		return nil
	}

	return s.cluster
}

func (s *Server) isClusterBootstrapped() bool {
	return nil != s.GetCellCluster()
}

func (s *Server) bootstrapCluster(req *pb.BootstrapClusterReq) (*pb.BootstrapClusterRsp, error) {
	if s.isClusterBootstrapped() {
		return &pb.BootstrapClusterRsp{
			AlreadyBootstrapped: true,
		}, nil
	}

	store, cell, err := s.checkForBootstrap(req)
	if err != nil {
		return nil, err
	}

	rsp, err := s.cluster.doBootstrap(store, cell)
	if err != nil {
		return nil, err
	}

	err = s.cluster.start()
	if err != nil {
		return nil, err
	}

	return rsp, nil
}

// GetClusterID returns cluster id
func (s *Server) GetClusterID() uint64 {
	return s.clusterID
}

func (s *Server) checkForBootstrap(req *pb.BootstrapClusterReq) (*meta.StoreMeta, *meta.CellMeta, error) {
	clusterID := s.GetClusterID()

	data := req.GetStore()
	if data.GetData() == nil {
		return nil, nil, errors.Errorf("missing store meta for bootstrap, clusterID=<%d>", clusterID)
	}
	store, err := meta.UnmarshalStoreMeta(data.GetData())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "unmarshal store meta failure for bootstrap cluster, clusterID=<%d>", clusterID)
	} else if store.ID == 0 {
		return nil, nil, errors.New("invalid zero store id for bootstrap cluster")
	}

	data = req.GetCell()
	if data.GetData() == nil {
		return nil, nil, errors.Errorf("missing cell meta for bootstrap, clusterID=<%d>",
			clusterID)
	}
	cell, err := meta.UnmarshalCellMeta(data.GetData())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "unmarshal cell meta failure for bootstrap cluster, clusterID=<%d>",
			clusterID)
	} else if cell.ID == 0 {
		return nil, nil, errors.New("invalid zero cell id for bootstrap cluster")
	} else if len(cell.Peers) == 0 || len(cell.Peers) != 1 {
		return nil, nil, errors.Errorf("invalid first cell peer count must be 1, count=<%d> clusterID=<%d>",
			len(cell.Peers),
			clusterID)
	} else if cell.Peers[0].ID == 0 {
		return nil, nil, errors.New("invalid zero peer id for bootstrap cluster")
	} else if cell.Peers[0].StoreID != store.ID {
		return nil, nil, errors.Errorf("invalid cell store id for bootstrap cluster, cell=<%d> expect=<%d> clusterID=<%d>",
			cell.Peers[0].StoreID,
			store.ID,
			clusterID)
	} else if cell.Peers[0].ID != cell.ID {
		return nil, nil, errors.Errorf("first cell peer must be self, self=<%d> peer=<%d>",
			cell.ID,
			cell.Peers[0].ID)
	}

	return store, cell, nil
}

// CellCluster is used for cluster config management.
type CellCluster struct {
	mux     sync.RWMutex
	s       *Server
	cache   *cache
	running bool
}

func newCellCluster(s *Server) *CellCluster {
	return &CellCluster{
		s:     s,
		cache: newCache(),
	}
}

func (c *CellCluster) doBootstrap(store *meta.StoreMeta, cell *meta.CellMeta) (*pb.BootstrapClusterRsp, error) {
	cluster := &meta.ClusterMeta{
		ID:          c.s.GetClusterID(),
		MaxReplicas: c.s.cfg.MaxReplicas,
	}

	ok, err := c.s.store.SetClusterBootstrapped(c.s.GetClusterID(), cluster, store, cell)
	if err != nil {
		return nil, err
	}

	return &pb.BootstrapClusterRsp{
		AlreadyBootstrapped: !ok,
	}, nil
}

func (c *CellCluster) isRunning() bool {
	c.mux.RLock()
	defer c.mux.RUnlock()

	return c.running
}

func (c *CellCluster) start() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.running {
		log.Warnf("cell-cluster: cell cluster is already started")
		return nil
	}

	clusterID := c.s.GetClusterID()

	// Here, we will load meta info from store.
	// If the cluster is not bootstrapped, the running flag is not set to true
	cluster, err := c.s.store.LoadClusterMeta(clusterID)
	if err != nil {
		return err
	}
	// cluster is not bootstrapped, skipped
	if nil == cluster {
		log.Warn("cell-cluster: start cluster skipped, cluster is not bootstapped")
		return nil
	}
	c.cache.cluster = newClusterRuntime(cluster)

	err = c.s.store.LoadStoreMeta(clusterID, batchLimit, c.cache.addStore)
	if err != nil {
		return err
	}

	err = c.s.store.LoadCellMeta(clusterID, batchLimit, c.cache.addCell)
	if err != nil {
		return err
	}

	log.Debugf("cell-cluster: load cluster meta succ, cache=<%v>", *c.cache)

	c.running = true
	log.Info("cell-cluster: cell cluster started.")
	return nil
}
