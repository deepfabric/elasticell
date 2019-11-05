package proxy

import (
	"bytes"
	"fmt"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/fagongzi/goetty/protocol/redis"
)

func (p *RedisProxy) doMGet(rs *redisSession, cmd redis.Command) (bool, error) {
	n := len(cmd.Args())

	if n < 2 {
		return false, nil
	}

	id := newID()
	rs.addAggregation(id, newAggregationReq(n, p.doMGetMerge, nil))

	for idx, key := range cmd.Args() {
		cmd := redis.Command([][]byte{cmdGet, key})
		p.addToForward(newReqUUID(bytes.Join([][]byte{id, []byte(fmt.Sprintf("%d", idx))}, sep), cmd, rs))
	}

	return true, nil
}

func (p *RedisProxy) doMGetMerge(args [][]byte, rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	n := len(rsps)
	value := make([][]byte, n, n)
	for idx, rsp := range rsps {
		value[idx] = rsp.BulkResult
	}

	return &raftcmdpb.Response{
		SliceArrayResult: value,
	}
}
