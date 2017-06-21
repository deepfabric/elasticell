package pdserver

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
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

func (s *Server) bootstrapCluster(req *pdpb.BootstrapClusterReq) (*pdpb.BootstrapClusterRsp, error) {
	if s.isClusterBootstrapped() {
		return &pdpb.BootstrapClusterRsp{
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

func (s *Server) putStore(req *pdpb.PutStoreReq) (*pdpb.PutStoreRsp, error) {
	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	err := s.checkStore(req.Store.ID)
	if err != nil {
		return nil, err
	}

	err = c.doPutStore(req.Store)
	if err != nil {
		return nil, err
	}

	log.Debugf("cell-cluster: put store ok, store=<%+v>", req.Store)

	return &pdpb.PutStoreRsp{}, nil
}

func (s *Server) getStore(req *pdpb.GetStoreReq) (*pdpb.GetStoreRsp, error) {
	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	store := c.cache.getStore(req.StoreID)
	if store == nil {
		return nil, errStoreNotFound
	}

	return &pdpb.GetStoreRsp{
		Store: store.store,
	}, nil
}

func (s *Server) cellHeartbeat(req *pdpb.CellHeartbeatReq) (*pdpb.CellHeartbeatRsp, error) {
	cluster := s.GetCellCluster()
	if nil == cluster {
		return nil, errNotBootstrapped
	}

	if req.GetLeader() == nil && len(req.Cell.Peers) != 1 {
		return nil, errRPCReq
	}

	if req.Cell.ID == pd.ZeroID {
		return nil, errRPCReq
	}

	cr := newCellRuntimeInfo(req.Cell, req.Leader)
	cr.downPeers = req.DownPeers
	cr.pendingPeers = req.PendingPeers

	return cluster.doCellHeartbeat(cr)
}

func (s *Server) storeHeartbeat(req *pdpb.StoreHeartbeatReq) (*pdpb.StoreHeartbeatRsp, error) {
	if req.Stats == nil {
		return nil, fmt.Errorf("invalid store heartbeat command, but %+v", req)
	}

	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	err := s.checkStore(req.Stats.StoreID)
	if err != nil {
		return nil, err
	}

	return c.doStoreHeartbeat(req)
}

func (s *Server) askSplit(req *pdpb.AskSplitReq) (*pdpb.AskSplitRsp, error) {
	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	return c.doAskSplit(req)
}

func (s *Server) reportSplit(req *pdpb.ReportSplitReq) (*pdpb.ReportSplitRsp, error) {
	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	return c.doReportSplit(req)
}

func (s *Server) getLastRanges(req *pdpb.GetLastRangesReq) (*pdpb.GetLastRangesRsp, error) {
	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	return c.doGetLastRanges(req)
}

// GetClusterID returns cluster id
func (s *Server) GetClusterID() uint64 {
	return s.clusterID
}

func (s *Server) checkForBootstrap(req *pdpb.BootstrapClusterReq) (metapb.Store, metapb.Cell, error) {
	clusterID := s.GetClusterID()

	store := req.GetStore()
	if store.ID == pd.ZeroID {
		return metapb.Store{}, metapb.Cell{}, errors.New("invalid zero store id for bootstrap cluster")
	}

	cell := req.GetCell()
	if cell.ID == pd.ZeroID {
		return metapb.Store{}, metapb.Cell{}, errors.New("invalid zero cell id for bootstrap cluster")
	} else if len(cell.Peers) == 0 || len(cell.Peers) != 1 {
		return metapb.Store{}, metapb.Cell{}, errors.Errorf("invalid first cell peer count must be 1, count=<%d> clusterID=<%d>",
			len(cell.Peers),
			clusterID)
	} else if cell.Peers[0].ID == pd.ZeroID {
		return metapb.Store{}, metapb.Cell{}, errors.New("invalid zero peer id for bootstrap cluster")
	} else if cell.Peers[0].StoreID != store.ID {
		return metapb.Store{}, metapb.Cell{}, errors.Errorf("invalid cell store id for bootstrap cluster, cell=<%d> expect=<%d> clusterID=<%d>",
			cell.Peers[0].StoreID,
			store.ID,
			clusterID)
	}

	return store, cell, nil
}

// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func (s *Server) checkStore(storeID uint64) error {
	c := s.GetCellCluster()

	store := c.cache.getStore(storeID)

	if store != nil && store.store.State == metapb.Tombstone {
		return errTombstoneStore
	}

	return nil
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
	c.coordinator.run()
	return c
}

func (c *CellCluster) doBootstrap(store metapb.Store, cell metapb.Cell) (*pdpb.BootstrapClusterRsp, error) {
	cluster := metapb.Cluster{
		ID:          c.s.GetClusterID(),
		MaxReplicas: c.s.cfg.Schedule.MaxReplicas,
	}

	ok, err := c.s.store.SetClusterBootstrapped(c.s.GetClusterID(), cluster, store, cell)
	if err != nil {
		return nil, err
	}

	return &pdpb.BootstrapClusterRsp{
		AlreadyBootstrapped: !ok,
	}, nil
}

func (c *CellCluster) doCellHeartbeat(cr *cellRuntimeInfo) (*pdpb.CellHeartbeatRsp, error) {
	err := c.cache.doCellHeartbeat(cr)
	if err != nil {
		return nil, err
	}

	if len(cr.cell.Peers) == 0 {
		return nil, errRPCReq
	}

	rsp := c.coordinator.dispatch(c.cache.getCell(cr.cell.ID))
	if rsp == nil {
		return emptyRsp, nil
	}

	return rsp, nil
}

func (c *CellCluster) doStoreHeartbeat(req *pdpb.StoreHeartbeatReq) (*pdpb.StoreHeartbeatRsp, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	storeID := req.Stats.StoreID
	store := c.cache.getStore(storeID)
	if nil == store {
		return nil, fmt.Errorf("store<%d> not found", storeID)
	}

	store.status.stats = req.Stats
	store.status.LeaderCount = uint32(c.cache.cc.getStoreLeaderCount(storeID))
	store.status.LastHeartbeatTS = time.Now()

	c.cache.setStore(store)
	return &pdpb.StoreHeartbeatRsp{}, nil
}

func (c *CellCluster) doPutStore(store metapb.Store) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if store.ID == pd.ZeroID {
		return fmt.Errorf("invalid for put store: <%+v>", store)
	}

	err := c.cache.foreachStore(func(s *storeRuntimeInfo) (bool, error) {
		if s.isTombstone() {
			return true, nil
		}

		if s.store.ID != store.ID && s.store.Address == store.Address {
			return false, fmt.Errorf("duplicated store address: %+v, already registered by %+v",
				store,
				s.store)
		}

		return true, nil
	})

	if err != nil {
		return err
	}

	old := c.cache.getStore(store.ID)
	if old == nil {
		old = newStoreRuntime(store)
	} else {
		old.store.Address = store.Address
		old.store.Lables = store.Lables
	}

	for _, k := range c.s.cfg.Schedule.LocationLabels {
		if v := old.getLabelValue(k); len(v) == 0 {
			return fmt.Errorf("missing location label %q in store %+v", k, old)
		}
	}

	err = c.s.store.SetStoreMeta(c.s.GetClusterID(), old.store)
	if err != nil {
		return err
	}

	c.cache.setStore(old)
	return nil
}

func (c *CellCluster) doAskSplit(req *pdpb.AskSplitReq) (*pdpb.AskSplitRsp, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	cr := c.cache.searchCell(req.Cell.Start)
	if cr == nil {
		return nil, errors.New("cell not found")
	}

	// If the request epoch is less than current cell epoch, then returns an error.
	if req.Cell.Epoch.CellVer < cr.cell.Epoch.CellVer ||
		req.Cell.Epoch.ConfVer < cr.cell.Epoch.ConfVer {
		return nil, errors.Errorf("invalid cell epoch, request: %v, currenrt: %v",
			req.Cell.Epoch,
			cr.cell.Epoch)
	}

	newCellID, err := c.s.idAlloc.newID()
	if err != nil {
		return nil, err
	}

	cnt := len(req.Cell.Peers)
	peerIDs := make([]uint64, cnt)
	for index := 0; index < cnt; index++ {
		if peerIDs[index], err = c.s.idAlloc.newID(); err != nil {
			return nil, err
		}
	}

	return &pdpb.AskSplitRsp{
		NewCellID:  newCellID,
		NewPeerIDs: peerIDs,
	}, nil
}

func (c *CellCluster) doReportSplit(req *pdpb.ReportSplitReq) (*pdpb.ReportSplitRsp, error) {
	left := req.Left
	right := req.Right

	err := c.checkSplitRegion(&left, &right)
	if err != nil {
		log.Warnf("cell-cluster:report split cell is invalid, req=<%+v> errors:\n %+v",
			req,
			err)
		return nil, err
	}

	return &pdpb.ReportSplitRsp{}, nil
}

func (c *CellCluster) doGetLastRanges(req *pdpb.GetLastRangesReq) (*pdpb.GetLastRangesRsp, error) {
	c.mux.RLock()
	defer c.mux.RUnlock()

	var ranges []*pdpb.Range
	for _, cr := range c.cache.cc.cells {
		if cr.leader != nil {
			ranges = append(ranges, &pdpb.Range{
				Cell:        cr.cell,
				LeaderStore: c.cache.getStore(cr.leader.StoreID).store,
			})
		}
	}

	return &pdpb.GetLastRangesRsp{
		Ranges: ranges,
	}, nil
}

func (c *CellCluster) checkSplitRegion(left *metapb.Cell, right *metapb.Cell) error {
	if !bytes.Equal(left.End, right.Start) {
		return errors.New("invalid split cell")
	}

	if len(right.End) == 0 || bytes.Compare(left.Start, right.End) < 0 {
		return nil
	}

	return errors.New("invalid split cell")
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

	err = c.s.store.LoadCellMeta(clusterID, batchLimit, c.cache.addCellFromMeta)
	if err != nil {
		return err
	}

	log.Debugf("cell-cluster: load cluster meta succ, cache=<%v>", c.cache)

	c.running = true
	log.Info("cell-cluster: cell cluster started.")
	return nil
}
