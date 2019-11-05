package proxy

import (
	"errors"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/util/format"
)

var (
	errInvalidCommand = errors.New("invalid command")
)

func (p *RedisProxy) doMGet(rs *redisSession, cmd redis.Command) (bool, error) {
	n := len(cmd.Args())
	if n < 2 {
		return false, errInvalidCommand
	}

	id := newID()
	rs.addAggregation(id, newAggregationReq(n, p.doMGetMerge, nil))

	for idx, key := range cmd.Args() {
		cmd := redis.Command([][]byte{cmdGet, key})
		p.addToForward(newReqUUID(append(id, format.UInt64ToString(uint64(idx))...), cmd, rs))
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
