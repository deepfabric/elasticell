package proxy

import (
	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	credis "github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
)

type redisSession struct {
	sync.RWMutex

	session goetty.IOSession
	resps   *util.Queue
	addr    string

	aggLock      sync.RWMutex
	aggregations map[string]*aggregationReq
}

func newSession(session goetty.IOSession) *redisSession {
	return &redisSession{
		session:      session,
		resps:        &util.Queue{},
		addr:         session.RemoteAddr(),
		aggregations: make(map[string]*aggregationReq),
	}
}

func (rs *redisSession) close() {
	rs.Lock()
	rs.resps.Dispose()
	log.Infof("redis-[%s]: closed", rs.addr)
	rs.Unlock()
}

func (rs *redisSession) addAggregation(id []byte, req *aggregationReq) {
	rs.aggLock.Lock()
	rs.aggregations[string(id)] = req
	rs.aggLock.Unlock()
}

func (rs *redisSession) resp(rsp *raftcmdpb.Response) {
	if !isAggregationPart(rsp.UUID) {
		rs.resps.Put(rsp)
		return
	}

	id, index := parseAggregationPart(rsp.UUID)
	rs.aggLock.RLock()
	if req, ok := rs.aggregations[string(id)]; ok {
		if req.addPart(index, rsp) {
			rs.resps.Put(req.merge())
		}
	}
	rs.aggLock.RUnlock()
}

func (rs *redisSession) errorResp(err error) {
	rs.resp(&raftcmdpb.Response{
		ErrorResult: util.StringToSlice(err.Error()),
	})
}

func (rs *redisSession) writeLoop() {
	items := make([]interface{}, batch, batch)

	for {
		n, err := rs.resps.Get(batch, items)
		if nil != err {
			return
		}

		rs.RLock()
		if !rs.session.IsConnected() {
			rs.RUnlock()
			return
		}

		buf := rs.session.OutBuf()
		for i := int64(0); i < n; i++ {
			rs.doResp(items[i].(*raftcmdpb.Response), buf)
		}
		rs.session.Flush()
		rs.RUnlock()
	}
}

func (rs *redisSession) doResp(resp *raftcmdpb.Response, buf *goetty.ByteBuf) {
	if resp.ErrorResult != nil {
		redis.WriteError(resp.ErrorResult, buf)
	}

	if resp.ErrorResults != nil {
		for _, err := range resp.ErrorResults {
			redis.WriteError(err, buf)
		}
	}

	if resp.BulkResult != nil || resp.HasEmptyBulkResult != nil {
		redis.WriteBulk(resp.BulkResult, buf)
	}

	if resp.FvPairArrayResult != nil || resp.HasEmptyFVPairArrayResult != nil {
		credis.WriteFVPairArray(resp.FvPairArrayResult, buf)
	}

	if resp.IntegerResult != nil {
		redis.WriteInteger(*resp.IntegerResult, buf)
	}

	if resp.ScorePairArrayResult != nil || resp.HasEmptyScorePairArrayResult != nil {
		credis.WriteScorePairArray(resp.ScorePairArrayResult, *resp.Withscores, buf)
	}

	if resp.SliceArrayResult != nil || resp.HasEmptySliceArrayResult != nil {
		redis.WriteSliceArray(resp.SliceArrayResult, buf)
	}

	if resp.StatusResult != nil {
		redis.WriteStatus(resp.StatusResult, buf)
	}

	if resp.DocArrayResult != nil || resp.HasEmptyDocArrayResult != nil {
		credis.WriteDocArray(resp.DocArrayResult, buf)
	}

	log.Debugf("redis-[%s]: response normal, resp=<%+v>",
		rs.addr,
		resp)
}
