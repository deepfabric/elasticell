package pdserver

import (
	"sync"

	pb "github.com/deepfabric/elasticell/pkg/pdpb"
)

// CellCluster is used for cluster config management.
type CellCluster struct {
	mux     sync.RWMutex
	s       *Server
	running bool
}

func newCellCluster(s *Server) *CellCluster {
	return &CellCluster{
		s: s,
	}
}

func (c *CellCluster) isRunning() bool {
	c.mux.RLock()
	c.mux.RUnlock()

	return c.running
}

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
	rsp := new(pb.BootstrapClusterRsp)

	// check if cluster is already bootstrapped
	if s.isClusterBootstrapped() {
		rsp.AlreadyBootstrapped = true
		return rsp, nil
	}

	// many kv node will call this at same time, only one can succ.
	ok, err := s.store.SetClusterBootstrapped()
	if err != nil {
		return nil, err
	}

	// other node succ, return already bootstrapped
	if !ok {
		return &pb.BootstrapClusterRsp{
			AlreadyBootstrapped: true,
		}, nil
	}

	return &pb.BootstrapClusterRsp{}, nil
}
