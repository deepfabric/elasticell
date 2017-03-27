package pdserver

import (
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	meta "github.com/deepfabric/elasticell/pkg/pb/metapb"
	pb "github.com/deepfabric/elasticell/pkg/pb/pdpb"
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

func (s *Server) cellHeartbeat(req *pb.CellHeartbeatReq) (*pb.CellHeartbeatRsp, error) {
	if req.GetLeader() == nil && len(req.Cell.Peers) != 1 {
		return nil, errRPCReq
	}

	// TODO: for peer is down or pending

	if req.Cell.Id == 0 {
		return nil, errRPCReq
	}

	cluster := s.GetCellCluster()
	return cluster.doCellHeartbeat(req.Cell)
}

// GetClusterID returns cluster id
func (s *Server) GetClusterID() uint64 {
	return s.clusterID
}

func (s *Server) checkForBootstrap(req *pb.BootstrapClusterReq) (meta.Store, meta.Cell, error) {
	clusterID := s.GetClusterID()

	store := req.GetStore()
	if store.Id == 0 {
		return meta.Store{}, meta.Cell{}, errors.New("invalid zero store id for bootstrap cluster")
	}

	cell := req.GetCell()
	if cell.Id == 0 {
		return meta.Store{}, meta.Cell{}, errors.New("invalid zero cell id for bootstrap cluster")
	} else if len(cell.Peers) == 0 || len(cell.Peers) != 1 {
		return meta.Store{}, meta.Cell{}, errors.Errorf("invalid first cell peer count must be 1, count=<%d> clusterID=<%d>",
			len(cell.Peers),
			clusterID)
	} else if cell.Peers[0].Id == 0 {
		return meta.Store{}, meta.Cell{}, errors.New("invalid zero peer id for bootstrap cluster")
	} else if cell.Peers[0].StoreID != store.Id {
		return meta.Store{}, meta.Cell{}, errors.Errorf("invalid cell store id for bootstrap cluster, cell=<%d> expect=<%d> clusterID=<%d>",
			cell.Peers[0].StoreID,
			store.Id,
			clusterID)
	} else if cell.Peers[0].Id != cell.Id {
		return meta.Store{}, meta.Cell{}, errors.Errorf("first cell peer must be self, self=<%d> peer=<%d>",
			cell.Id,
			cell.Peers[0].Id)
	}

	return store, cell, nil
}

// CellCluster is used for cluster config management.
type CellCluster struct {
	mux         sync.RWMutex
	s           *Server
	coordinator *coordinator
	cache       *cache
	running     bool
}

func newCellCluster(s *Server) *CellCluster {
	c := &CellCluster{
		s:     s,
		cache: newCache(s.clusterID, s.store, s.idAlloc),
	}

	c.coordinator = newCoordinator(s.cfg, c.cache)

	return c
}

func (c *CellCluster) doBootstrap(store meta.Store, cell meta.Cell) (*pb.BootstrapClusterRsp, error) {
	cluster := meta.Cluster{
		Id:          c.s.GetClusterID(),
		MaxReplicas: c.s.cfg.getMaxReplicas(),
	}

	ok, err := c.s.store.SetClusterBootstrapped(c.s.GetClusterID(), cluster, store, cell)
	if err != nil {
		return nil, err
	}

	return &pb.BootstrapClusterRsp{
		AlreadyBootstrapped: !ok,
	}, nil
}

func (c *CellCluster) doCellHeartbeat(cell meta.Cell) (*pb.CellHeartbeatRsp, error) {
	err := c.cache.doCellHeartbeat(cell)
	if err != nil {
		return nil, err
	}

	if len(cell.Peers) == 0 {
		return nil, errRPCReq
	}

	rsp := c.coordinator.dispatch(c.cache.getCell(cell.Id))
	if rsp == nil {
		return emptyRsp, nil
	}

	return rsp, nil
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
	c.cache.cluster = newClusterRuntime(*cluster)

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
