package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/pdapi"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/net/context"
)

const (
	batch = 64
)

var (
	pingReq = &raftcmdpb.Request{
		Cmd: [][]byte{[]byte("ping")},
	}
)

type req struct {
	rs      *redisSession
	raftReq *raftcmdpb.Request
	retries int
}

func newReqUUID(id []byte, cmd redis.Command, rs *redisSession) *req {
	return &req{
		raftReq: &raftcmdpb.Request{
			UUID: id,
			Cmd:  cmd,
		},
		rs:      rs,
		retries: 0,
	}
}

func newReq(cmd redis.Command, rs *redisSession) *req {
	return newReqUUID(newID(), cmd, rs)
}

func newID() []byte {
	return uuid.NewV4().Bytes()
}

func (r *req) errorDone(err error) {
	if r.rs != nil {
		r.rs.errorResp(err)
	}
}

func (r *req) done(rsp *raftcmdpb.Response) {
	if r.rs != nil {
		r.rs.resp(rsp)
	}
}

// RedisProxy is a redis proxy
type RedisProxy struct {
	sync.RWMutex

	cfg             *Cfg
	svr             *goetty.Server
	pdClient        *pd.Client
	watcher         *pd.Watcher
	aggregationCmds map[string]func(*redisSession, redis.Command) (bool, error)
	supportCmds     map[string]struct{}
	keyConvertFun   func([]byte, func([]byte) metapb.Cell) metapb.Cell
	ranges          *util.CellTree
	stores          map[uint64]*metapb.Store
	cellLeaderAddrs map[uint64]string   // cellid -> leader peer store addr
	bcs             map[string]*backend // store addr -> netconn
	routing         *routing            // uuid -> session
	syncEpoch       uint64
	reqs            []*util.Queue
	retries         *util.Queue
	pings           chan string
	bcAddrs         []string // store addrs
	rrNext          int64    // round robin of bcAddrs

	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan struct{}
}

// NewRedisProxy returns a redisp proxy
func NewRedisProxy(cfg *Cfg) *RedisProxy {
	client, err := pd.NewClient(fmt.Sprintf("proxy-%s", cfg.Addr), cfg.PDAddrs...)
	if err != nil {
		log.Fatalf("bootstrap: create pd client failed, errors:\n%+v", err)
	}

	redisSvr := goetty.NewServer(cfg.Addr,
		goetty.WithServerDecoder(redis.NewRedisDecoder()),
		goetty.WithServerEncoder(goetty.NewEmptyEncoder()))

	watcher := pd.NewWatcher(client,
		cfg.AddrNotify,
		pd.EventFlagCell|pd.EventFlagStore|pd.EventInit,
		time.Duration(cfg.WatcherHeartbeatSec)*time.Second)

	p := &RedisProxy{
		pdClient:        client,
		cfg:             cfg,
		svr:             redisSvr,
		watcher:         watcher,
		aggregationCmds: make(map[string]func(*redisSession, redis.Command) (bool, error)),
		supportCmds:     make(map[string]struct{}),
		routing:         newRouting(),
		ranges:          util.NewCellTree(),
		stores:          make(map[uint64]*metapb.Store),
		cellLeaderAddrs: make(map[uint64]string),
		bcs:             make(map[string]*backend),
		stopC:           make(chan struct{}),
		reqs:            make([]*util.Queue, cfg.WorkerCount),
		retries:         &util.Queue{},
		pings:           make(chan string),
		bcAddrs:         make([]string, 0),
	}

	p.init()

	return p
}

// Start starts the proxy
func (p *RedisProxy) Start() error {
	go p.listenToStop()
	go p.readyToHandleReq(p.ctx)

	return p.svr.Start(p.doConnection)
}

// Stop stop the proxy
func (p *RedisProxy) Stop() {
	p.stopWG.Add(1)
	p.stopC <- struct{}{}
	p.stopWG.Wait()
}

func (p *RedisProxy) listenToStop() {
	<-p.stopC
	p.doStop()
}

func (p *RedisProxy) init() {
	p.ctx, p.cancel = context.WithCancel(context.TODO())

	p.initKeyConvert()
	p.initSupportCMDs()
	p.initWatcher()
	p.initQueues()
}

func (p *RedisProxy) doStop() {
	p.stopOnce.Do(func() {
		defer p.stopWG.Done()

		log.Infof("stop: start to stop redis proxy")
		for _, bc := range p.bcs {
			bc.close(true)
			log.Infof("stop: store connection closed, addr=<%s>", bc.addr)
		}

		p.watcher.Stop()
		p.svr.Stop()
		log.Infof("stop: tcp listen stopped")

		p.cancel()
	})
}

func (p *RedisProxy) initWatcher() {
	err := p.watcher.Start()
	if err != nil {
		log.Fatalf("bootstrap: init watcher failed, errors:\n%+v", err)
	}

	go func() {
		for {
			event, err := p.watcher.Ready()
			if err != nil {
				return
			}

			switch event.Event {
			case pd.EventInit:
				p.refreshRanges()
				p.refreshStores()
			case pd.EventCellCreated:
				p.refreshRange(event.CellEvent.Range)
			case pd.EventCellLeaderChanged:
				p.refreshRange(event.CellEvent.Range)
			case pd.EventCellRangeChaned:
				p.refreshRange(event.CellEvent.Range)
			case pd.EventCellPeersChaned:
				p.refreshRange(event.CellEvent.Range)
			case pd.EventStoreUp:
			case pd.EventStoreDown:
			case pd.EventStoreTombstone:
			}
		}
	}()
}

func (p *RedisProxy) initQueues() {
	for index := uint64(0); index < p.cfg.WorkerCount; index++ {
		p.reqs[index] = &util.Queue{}
	}
}

func (p *RedisProxy) doConnection(session goetty.IOSession) error {
	addr := session.RemoteAddr()
	log.Infof("redis-[%s]: connected", addr)

	// every client has 2 goroutines, read, write
	rs := newSession(session)
	go rs.writeLoop()
	defer rs.close()

	for {
		r, err := session.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			log.Errorf("redis-[%s]: read from cli failed, errors\n %+v",
				addr,
				err)
			return err
		}

		cmd := r.(redis.Command)
		if log.DebugEnabled() {
			log.Debugf("redis-[%s]: read a cmd: %s", addr, cmd.ToString())
		}

		cmdStr := cmd.CmdString()
		_, ok := p.supportCmds[cmdStr]
		if !ok {
			rs.errorResp(fmt.Errorf("command not support: %s", cmdStr))
			continue
		}

		if fn, ok := p.aggregationCmds[cmdStr]; ok {
			need, err := fn(rs, cmd)
			if err != nil {
				rs.errorResp(err)
				continue
			}

			if need {
				continue
			}
		}

		p.addToForward(newReq(cmd, rs))
	}
}

func (p *RedisProxy) initKeyConvert() {
	rsp, err := p.pdClient.GetInitParams(context.TODO(), new(pdpb.GetInitParamsReq))
	if err != nil {
		log.Fatalf("bootstrap: get init params failed, errors:\n%+v", err)
	}

	params := &pdapi.InitParams{
		InitCellCount: 1,
	}

	if len(rsp.Params) > 0 {
		err = json.Unmarshal(rsp.Params, params)
		if err != nil {
			log.Fatalf("bootstrap: create pd client failed, errors:\n%+v", err)
		}
	}

	if params.InitCellCount > 1 {
		p.keyConvertFun = util.Uint64Convert
	} else {
		p.keyConvertFun = util.NoConvert
	}
}

func (p *RedisProxy) initSupportCMDs() {
	for _, cmd := range p.cfg.SupportCMDs {
		p.supportCmds[cmd] = struct{}{}
	}

	p.aggregationCmds["mget"] = p.doMGet
	p.aggregationCmds["mset"] = p.doMSet
}

func (p *RedisProxy) refreshStores() {
	var stores []string
	var rsp *pdpb.ListStoreRsp
	var err error
	if rsp, err = p.pdClient.ListStore(context.TODO(), &pdpb.ListStoreReq{}); err != nil {
		log.Errorf("ListStore failed with error\n%+v", err)
	}
	stores = make([]string, 0)
	for _, s := range rsp.Stores {
		stores = append(stores, s.ClientAddress)
	}
	sort.Strings(stores)
	p.Lock()
	p.bcAddrs = stores
	p.Unlock()
}

func (p *RedisProxy) refreshRanges() {
	old := p.getSyncEpoch()
	log.Infof("pd-sync: try to sync, epoch=<%d>", old)

	if old < p.syncEpoch {
		log.Infof("pd-sync: already sync, skip, old=<%d> new=<%d>", old, p.syncEpoch)
		return
	}

	p.Lock()
	rsp, err := p.pdClient.GetLastRanges(context.TODO(), &pdpb.GetLastRangesReq{})
	if err != nil {
		log.Fatalf("bootstrap: get cell ranges from pd failed, errors:\n%+v", err)
	}

	p.clean()
	for _, r := range rsp.Ranges {
		p.doRefreshRange(r)
	}
	p.syncEpoch++

	log.Infof("pd-sync: sync complete, epoch=%d", p.syncEpoch)
	p.Unlock()
}

func (p *RedisProxy) refreshRange(r *pdpb.Range) {
	p.Lock()
	p.doRefreshRange(r)
	p.syncEpoch++
	p.Unlock()
}

func (p *RedisProxy) doRefreshRange(r *pdpb.Range) {
	p.ranges.Update(r.Cell)
	p.cellLeaderAddrs[r.Cell.ID] = r.LeaderStore.ClientAddress
	p.stores[r.LeaderStore.ID] = &r.LeaderStore
}

func (p *RedisProxy) clean() {
	p.stores = make(map[uint64]*metapb.Store)
	p.cellLeaderAddrs = make(map[uint64]string)
}

func (p *RedisProxy) getSyncEpoch() uint64 {
	p.RLock()
	v := p.syncEpoch
	p.RUnlock()
	return v
}

func (p *RedisProxy) addToPing(target string) {
	p.pings <- target
}

func (p *RedisProxy) retry(r *req) {
	r.retries++
	p.retries.Put(r)
}

func (p *RedisProxy) addToForward(r *req) {
	if r.raftReq == nil {
		log.Fatalf("bug: raft req cannot be nil")
	}

	if r.retries == 0 {
		r.raftReq.Epoch = p.getSyncEpoch()
	}

	if len(r.raftReq.Cmd) <= 1 {
		p.reqs[0].Put(r)
		return
	}

	c := p.ranges.Search(r.raftReq.Cmd[1])
	index := (p.cfg.WorkerCount - 1) & c.ID
	p.reqs[index].Put(r)
}

func (p *RedisProxy) readyToHandleReq(ctx context.Context) {
	for _, q := range p.reqs {
		go func(q *util.Queue) {
			log.Infof("bootstrap: handle redis command started")
			items := make([]interface{}, batch, batch)

			for {
				n, err := q.Get(batch, items)
				if nil != err {
					log.Infof("stop: handle redis command stopped")
					return
				}

				for i := int64(0); i < n; i++ {
					r := items[i].(*req)
					p.handleReq(r)
				}
			}
		}(q)
	}

	go func() {
		log.Infof("bootstrap: handle redis retries command started")
		items := make([]interface{}, batch, batch)

		for {
			n, err := p.retries.Get(batch, items)
			if nil != err {
				log.Infof("stop: handle redis retries command stopped")
				return
			}

			failed := 0
			for i := int64(0); i < n; i++ {
				r := items[i].(*req)
				if r.raftReq.Epoch < p.getSyncEpoch() {
					p.addToForward(r)
				} else {
					p.retries.Put(r)
					failed++
				}
			}

			if failed > 0 {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			for _, q := range p.reqs {
				q.Dispose()
			}

			close(p.pings)
			log.Infof("stop: handle redis command stopped")
			return
		case target := <-p.pings:
			if target != "" {
				p.forwardTo(target, &req{
					raftReq: pingReq,
				})
			}
		}
	}
}

func (p *RedisProxy) handleReq(r *req) {
	if r.retries > 0 {
		// If epoch is not stale, wait next
		if r.raftReq.Epoch >= p.getSyncEpoch() {
			p.retries.Put(r)
			return
		}
	}

	target := ""
	var cellID uint64

	if len(r.raftReq.Cmd) <= 1 {
		target = p.getRandomStoreAddr()
	} else if strings.ToLower(string(r.raftReq.Cmd[0])) == "query" {
		target = p.getRRStoreAddr()
	} else {
		target, cellID = p.getLeaderStoreAddr(r.raftReq.Cmd[1])
	}

	if log.DebugEnabled() {
		log.Debugf("req: handle req, uuid=<%+v>, cell=<%d>, bc=<%s> times=<%d> cmd=<%v>",
			r.raftReq.UUID,
			cellID,
			target,
			r.retries,
			r.raftReq.Cmd)
	}

	if target == "" {
		log.Debugf("req: leader not found for key, uuid=<%+v> key=<%v>",
			r.raftReq.UUID,
			r.raftReq.Cmd[1])
		p.retry(r)
		return
	}

	err := p.forwardTo(target, r)
	if err != nil {
		log.Errorf("req: forward failed, uuid=<%+v> error=<%s>",
			r.raftReq.UUID,
			err)
		p.retry(r)
		return
	}
}

func (p *RedisProxy) forwardTo(addr string, r *req) error {
	bc, err := p.getConn(addr)
	if err != nil {
		return errors.Wrapf(err, "getConn")
	}

	r.raftReq.Epoch = p.getSyncEpoch()

	if nil != r.rs {
		p.routing.put(r.raftReq.UUID, r)
	}

	if nil != r.raftReq && len(r.raftReq.UUID) > 0 {
		log.Debugf("req: added to backend queue, uuid=<%+v>", r.raftReq.UUID)
	}

	err = bc.addReq(r)
	if err != nil {
		p.routing.delete(r.raftReq.UUID)
		return errors.Wrapf(err, "writeTo")
	}

	return nil
}

func (p *RedisProxy) onResp(rsp *raftcmdpb.Response) {
	r := p.routing.delete(rsp.UUID)
	if r != nil {
		if rsp.Type == raftcmdpb.RaftError {
			p.retry(r)
			return
		}

		r.done(rsp)
	} else if len(rsp.UUID) > 0 {
		log.Debugf("redis-resp: client maybe closed, ingore resp, uuid=<%+v>",
			rsp.UUID)
	}
}

func (p *RedisProxy) search(value []byte) metapb.Cell {
	return p.ranges.Search(value)
}

func (p *RedisProxy) getLeaderStoreAddr(key []byte) (string, uint64) {
	p.RLock()
	cell := p.keyConvertFun(key, p.search)
	addr := p.cellLeaderAddrs[cell.ID]
	p.RUnlock()

	return addr, cell.ID
}

func (p *RedisProxy) getRandomStoreAddr() string {
	p.RLock()
	var target *metapb.Cell
	p.ranges.Ascend(func(cell *metapb.Cell) bool {
		target = cell
		return false
	})
	addr := p.cellLeaderAddrs[target.ID]
	p.RUnlock()

	return addr
}

func (p *RedisProxy) getRRStoreAddr() (target string) {
	p.RLock()
	total := int64(len(p.bcAddrs))
	if total == 0 {
		p.RUnlock()
		return
	}
	idx := atomic.AddInt64(&p.rrNext, 1) - 1
	if idx >= total {
		idx %= total
	}
	target = p.bcAddrs[idx]
	p.RUnlock()
	log.Debugf("getRRStoreAddr picked #%d of %v", idx, p.bcAddrs)
	return
}
