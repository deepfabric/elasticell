package proxy

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/fagongzi/goetty/protocol/redis"
)

var (
	sep    = []byte("#")
	cmdSet = []byte("SET")
	cmdGet = []byte("GET")
)

type aggregationReq struct {
	reply   int
	parts   []*raftcmdpb.Response
	mergeFn func(...*raftcmdpb.Response) *raftcmdpb.Response
}

func newAggregationReq(n int, mergeFn func(...*raftcmdpb.Response) *raftcmdpb.Response) *aggregationReq {
	return &aggregationReq{
		reply:   n,
		parts:   make([]*raftcmdpb.Response, n, n),
		mergeFn: mergeFn,
	}
}

func (req *aggregationReq) addPart(index int, rsp *raftcmdpb.Response) bool {
	req.parts[index] = rsp
	req.reply--

	return req.reply == 0
}

func (req *aggregationReq) merge() *raftcmdpb.Response {
	return req.mergeFn(req.parts...)
}

func (p *RedisProxy) doMGet(rs *redisSession, cmd redis.Command) (bool, error) {
	n := len(cmd.Args())

	if n < 2 {
		return false, nil
	}

	id := newID()
	rs.addAggregation(id, newAggregationReq(n, p.doMGetMerge))

	for idx, key := range cmd.Args() {
		cmd := redis.Command([][]byte{cmdGet, key})
		p.addToForward(newReqUUID(bytes.Join([][]byte{id, []byte(fmt.Sprintf("%d", idx))}, sep), cmd, rs))
	}

	return true, nil
}

func (p *RedisProxy) doMGetMerge(rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	n := len(rsps)
	value := make([][]byte, n, n)
	for idx, rsp := range rsps {
		value[idx] = rsp.BulkResult
	}

	return &raftcmdpb.Response{
		SliceArrayResult: value,
	}
}

func (p *RedisProxy) doMSet(rs *redisSession, cmd redis.Command) (bool, error) {
	n := len(cmd.Args())
	if n < 2 {
		return false, nil
	}

	if n%2 != 0 {
		return false, fmt.Errorf("error args count: %d", n)
	}

	id := newID()
	rs.addAggregation(id, newAggregationReq(n/2, p.doMSetMerge))

	for i := 0; i < n; i += 2 {
		cmd := redis.Command([][]byte{cmdSet, cmd.Args()[i], cmd.Args()[i+1]})
		p.addToForward(newReqUUID(bytes.Join([][]byte{id, []byte(fmt.Sprintf("%d", i/2))}, sep), cmd, rs))
	}

	return true, nil
}

func (p *RedisProxy) doMSetMerge(rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	var value *raftcmdpb.Response

	for _, rsp := range rsps {
		if len(rsp.ErrorResult) > 0 {
			return rsp
		}

		value = rsp
	}

	return value
}

func isAggregationPart(id []byte) bool {
	return len(id) > 16
}

func parseAggregationPart(id []byte) ([]byte, int) {
	data := bytes.Split(id, sep)
	return data[0], parseStrInt64(data[1])
}

func parseStrInt64(data []byte) int {
	v, _ := strconv.ParseInt(string(data), 10, 64)
	return int(v)
}
