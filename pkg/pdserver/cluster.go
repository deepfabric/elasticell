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

	store, err := s.checkForBootstrap(req)
	if err != nil {
		return nil, err
	}

	rsp, err := s.cluster.doBootstrap(store, req.Cells)
	if err != nil {
		return nil, err
	}

	err = s.cluster.start()
	if err != nil {
		return nil, err
	}

	return rsp, nil
}

func (s *Server) listStore(req *pdpb.ListStoreReq) (*pdpb.ListStoreRsp, error) {
	c := s.GetCellCluster()
	if c == nil {
		return nil, errNotBootstrapped
	}

	rsp := &pdpb.ListStoreRsp{
		Stores: make([]*metapb.Store, 0),
	}
	storeInfos := c.cache.getStoreCache().getStores()
	for _, storeInfo := range storeInfos {
		rsp.Stores = append(rsp.Stores, &storeInfo.Meta)
	}

	log.Debugf("cell-cluster: list store ok, stores=<%+v>", rsp.Stores)
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

	store := c.cache.getStoreCache().getStore(req.StoreID)
	if store == nil {
		return nil, errStoreNotFound
	}

	return &pdpb.GetStoreRsp{
		Store: store.Meta,
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

	cr := newCellInfo(req.Cell, req.Leader)
	cr.DownPeers = req.DownPeers
	cr.PendingPeers = req.PendingPeers

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

	rsp, err := c.doStoreHeartbeat(req)
	if err != nil {
		return nil, err
	}
	rsp.Indices, err = s.ListIndex()
	return rsp, err
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

func (s *Server) registerWatcher(req *pdpb.RegisterWatcherReq) (*pdpb.RegisterWatcherRsp, error) {
	err := s.store.SetWatchers(s.GetClusterID(), req.Watcher)
	if err != nil {
		return nil, err
	}

	s.notifier.addWatcher(req.Watcher)
	return &pdpb.RegisterWatcherRsp{}, nil
}

func (s *Server) watcherHeartbeat(req *pdpb.WatcherHeartbeatReq) (*pdpb.WatcherHeartbeatRsp, error) {
	return &pdpb.WatcherHeartbeatRsp{
		Paused: s.notifier.watcherHeartbeat(req.Addr, req.Offset),
	}, nil
}

// GetClusterID returns cluster id
func (s *Server) GetClusterID() uint64 {
	return s.clusterID
}

// GetInitParamsValue returns cluster init params bytes
func (s *Server) GetInitParamsValue() ([]byte, error) {
	return s.store.GetInitParams(s.clusterID)
}

func (s *Server) checkForBootstrap(req *pdpb.BootstrapClusterReq) (metapb.Store, error) {
	clusterID := s.GetClusterID()

	store := req.GetStore()
	if store.ID == pd.ZeroID {
		return metapb.Store{}, errors.New("invalid zero store id for bootstrap cluster")
	}

	for _, cell := range req.Cells {
		if cell.ID == pd.ZeroID {
			return metapb.Store{}, errors.New("invalid zero cell id for bootstrap cluster")
		} else if len(cell.Peers) == 0 || len(cell.Peers) != 1 {
			return metapb.Store{}, errors.Errorf("invalid first cell peer count must be 1, count=<%d> clusterID=<%d>",
				len(cell.Peers),
				clusterID)
		} else if cell.Peers[0].ID == pd.ZeroID {
			return metapb.Store{}, errors.New("invalid zero peer id for bootstrap cluster")
		} else if cell.Peers[0].StoreID != store.ID {
			return metapb.Store{}, errors.Errorf("invalid cell store id for bootstrap cluster, cell=<%d> expect=<%d> clusterID=<%d>",
				cell.Peers[0].StoreID,
				store.ID,
				clusterID)
		}
	}

	return store, nil
}

// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func (s *Server) checkStore(storeID uint64) error {
	c := s.GetCellCluster()

	store := c.cache.getStoreCache().getStore(storeID)

	if store != nil && store.Meta.State == metapb.Tombstone {
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
		cache: newCache(s.clusterID, s.store, s.idAlloc, s.notifier),
	}

	c.coordinator = newCoordinator(s.cfg, c.cache)
	c.coordinator.run()
	return c
}

func (c *CellCluster) doBootstrap(store metapb.Store, cells []metapb.Cell) (*pdpb.BootstrapClusterRsp, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	cluster := metapb.Cluster{
		ID:          c.s.GetClusterID(),
		MaxReplicas: c.s.cfg.LimitReplicas,
	}

	ok, err := c.s.store.SetClusterBootstrapped(c.s.GetClusterID(), cluster, store, cells)
	if err != nil {
		return nil, err
	}

	return &pdpb.BootstrapClusterRsp{
		AlreadyBootstrapped: !ok,
	}, nil
}

func (c *CellCluster) doCellHeartbeat(cr *CellInfo) (*pdpb.CellHeartbeatRsp, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	err := c.cache.handleCellHeartbeat(cr)
	if err != nil {
		return nil, err
	}

	if len(cr.Meta.Peers) == 0 {
		return nil, errRPCReq
	}

	rsp := c.coordinator.dispatch(c.cache.getCellCache().getCell(cr.Meta.ID))
	if rsp == nil {
		return emptyRsp, nil
	}

	return rsp, nil
}

func (c *CellCluster) doStoreHeartbeat(req *pdpb.StoreHeartbeatReq) (*pdpb.StoreHeartbeatRsp, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	storeID := req.Stats.StoreID
	store := c.cache.getStoreCache().getStore(storeID)
	if nil == store {
		return nil, fmt.Errorf("store<%d> not found", storeID)
	}

	store.Status.Stats = req.Stats
	store.Status.LeaderCount = uint32(c.cache.cc.getStoreLeaderCount(storeID))
	store.Status.LastHeartbeatTS = time.Now()

	c.cache.getStoreCache().updateStoreInfo(store)
	return c.coordinator.dispatchStore(c.cache.getStoreCache().getStore(storeID)), nil
}

func (c *CellCluster) doPutStore(store metapb.Store) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if store.ID == pd.ZeroID {
		return fmt.Errorf("invalid for put store: <%+v>", store)
	}

	err := c.cache.getStoreCache().foreach(func(s *StoreInfo) (bool, error) {
		if s.isTombstone() {
			return true, nil
		}

		if s.Meta.ID != store.ID && s.Meta.Address == store.Address {
			return false, fmt.Errorf("duplicated store address: %+v, already registered by %+v",
				store,
				s.Meta)
		}

		return true, nil
	})

	if err != nil {
		return err
	}

	old := c.cache.getStoreCache().getStore(store.ID)
	if old == nil {
		old = newStoreInfo(store)
	} else {
		old.Meta.Address = store.Address
		old.Meta.Lables = store.Lables
	}

	for _, k := range c.s.cfg.LabelsLocation {
		if v := old.getLabelValue(k); len(v) == 0 {
			return fmt.Errorf("missing location label %q in store %+v", k, old)
		}
	}

	err = c.s.store.SetStoreMeta(c.s.GetClusterID(), old.Meta)
	if err != nil {
		return err
	}

	c.cache.getStoreCache().updateStoreInfo(old)
	c.cache.notifyStoreRange(old.Meta.ID)
	return nil
}

func (c *CellCluster) doAskSplit(req *pdpb.AskSplitReq) (*pdpb.AskSplitRsp, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	cr := c.cache.getCellCache().searchCell(req.Cell.Start)
	if cr == nil {
		return nil, errors.New("cell not found")
	}

	// If the request epoch is less than current cell epoch, then returns an error.
	if req.Cell.Epoch.CellVer < cr.Meta.Epoch.CellVer ||
		req.Cell.Epoch.ConfVer < cr.Meta.Epoch.ConfVer {
		return nil, errors.Errorf("invalid cell epoch, request: %v, currenrt: %v",
			req.Cell.Epoch,
			cr.Meta.Epoch)
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
	c.mux.Lock()
	defer c.mux.Unlock()

	left := req.Left
	right := req.Right

	err := c.checkSplitCell(&left, &right)
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
	c.cache.cc.foreach(func(cr *CellInfo) (bool, error) {
		if cr.LeaderPeer != nil {
			ranges = append(ranges, &pdpb.Range{
				Cell:        cr.Meta,
				LeaderStore: c.cache.getStoreCache().getStore(cr.LeaderPeer.StoreID).Meta,
			})
		}

		return true, nil
	})

	return &pdpb.GetLastRangesRsp{
		Ranges: ranges,
	}, nil
}

func (c *CellCluster) checkSplitCell(left *metapb.Cell, right *metapb.Cell) error {
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

	err = c.s.store.LoadStoreMeta(clusterID, batchLimit, c.cache.getStoreCache().createStoreInfo)
	if err != nil {
		return err
	}

	err = c.s.store.LoadCellMeta(clusterID, batchLimit, c.cache.getCellCache().createAndAdd)
	if err != nil {
		return err
	}

	log.Debugf("cell-cluster: load cluster meta succ, cache=<%v>", c.cache)

	c.running = true
	log.Info("cell-cluster: cell cluster started.")
	return nil
}
