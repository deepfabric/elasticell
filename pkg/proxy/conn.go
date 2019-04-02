package proxy

import (
	"sort"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/codec"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
)

var (
	errConnect            = errors.New("not connected")
	defaultConnectTimeout = time.Second * 5
)

type backend struct {
	sync.RWMutex

	p    *RedisProxy
	addr string
	conn goetty.IOSession
	reqs *util.Queue
}

func newBackend(p *RedisProxy, addr string, conn goetty.IOSession) *backend {
	bc := &backend{
		p:    p,
		addr: addr,
		conn: conn,
		reqs: &util.Queue{},
	}

	bc.writeLoop()
	return bc
}

func (bc *backend) isConnected() bool {
	bc.RLock()
	v := bc.conn != nil && bc.conn.IsConnected()
	bc.RUnlock()

	return v
}

func (bc *backend) connect() (bool, error) {
	bc.Lock()
	yes, err := bc.conn.Connect()
	bc.Unlock()
	log.Infof("backend-[%s]: connected", bc.addr)
	return yes, err
}

func (bc *backend) close(exit bool) {
	bc.Lock()

	if exit {
		reqs := bc.reqs.Dispose()
		for _, v := range reqs {
			r := v.(*req)
			r.errorDone(errConnect)
		}
	}

	if bc.conn != nil {
		bc.conn.Close()
	}
	bc.Unlock()
}

func (bc *backend) addReq(r *req) error {
	bc.Lock()

	if !bc.conn.IsConnected() {
		bc.Unlock()
		return errConnect
	}

	err := bc.reqs.Put(r)

	bc.Unlock()

	return err
}

func (bc *backend) readLoop() {
	go func() {
		log.Infof("backend-[%s]: start read loop", bc.addr)

		for {
			data, err := bc.conn.Read()
			if err != nil {
				log.Errorf("backend-[%s]: read error: %s",
					bc.addr,
					err)
				bc.close(false)
				log.Infof("backend-[%s]: exit read loop", bc.addr)
				return

			}
			rsp, ok := data.(*raftcmdpb.Response)
			if ok && len(rsp.UUID) > 0 {
				log.Debugf("backend-[%s]: read a response: uuid=<%+v> resp=<%+v>",
					bc.addr,
					rsp.UUID,
					rsp)

				bc.p.onResp(rsp)
			}
		}
	}()
}

func (bc *backend) writeLoop() {
	go func() {
		log.Infof("backend-[%s]: start write loop", bc.addr)

		items := make([]interface{}, batch, batch)

		for {
			n, err := bc.reqs.Get(batch, items)
			if err != nil {
				log.Infof("backend-[%s]: exit read loop", bc.addr)
				return
			}

			out := bc.conn.OutBuf()
			for i := int64(0); i < n; i++ {
				r := items[i].(*req)
				if log.DebugEnabled() {
					log.Debugf("backend-[%s]: ready to send: %s",
						bc.addr,
						r.raftReq.String())
				}
				codec.WriteProxyMessage(codec.RedisBegin, r.raftReq, out)
			}
			err = bc.conn.Flush()
			if err != nil {
				for i := int64(0); i < n; i++ {
					r := items[i].(*req)
					r.errorDone(err)
				}
				continue
			}
		}
	}()
}

func (p *RedisProxy) getConn(addr string) (*backend, error) {
	bc := p.getConnLocked(addr)
	if p.checkConnect(addr, bc) {
		return bc, nil
	}

	return bc, errConnect
}

func (p *RedisProxy) getConnLocked(addr string) *backend {
	p.RLock()
	bc := p.bcs[addr]
	p.RUnlock()

	if bc != nil {
		return bc
	}

	return p.createConn(addr)
}

func (p *RedisProxy) createConn(addr string) *backend {
	p.Lock()

	// double check
	if bc, ok := p.bcs[addr]; ok {
		p.Unlock()
		return bc
	}

	conn := goetty.NewConnector(addr,
		goetty.WithClientConnectTimeout(defaultConnectTimeout),
		goetty.WithClientDecoder(&codec.ProxyDecoder{}),
		goetty.WithClientEncoder(&codec.ProxyEncoder{}))
	b := newBackend(p, addr, conn)
	p.bcs[addr] = b

	// update p.bcAddrs
	p.bcAddrs = make([]string, 0)
	for addr := range p.bcs {
		p.bcAddrs = append(p.bcAddrs, addr)
	}
	sort.Strings(p.bcAddrs)
	p.Unlock()
	return b
}

func (p *RedisProxy) checkConnect(addr string, bc *backend) bool {
	if nil == bc {
		return false
	}

	p.Lock()
	if bc.isConnected() {
		p.Unlock()
		return true
	}

	ok, err := bc.connect()
	if err != nil {
		log.Errorf("transport: connect to store failure, target=<%s> errors:\n %+v",
			addr,
			err)
		p.Unlock()
		return false
	}

	bc.readLoop()
	p.Unlock()
	return ok
}
