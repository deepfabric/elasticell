package proxy

import (
	"strconv"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
)

var (
	cmdSet = []byte("SET")
	cmdGet = []byte("GET")
)

type aggregationReq struct {
	reply   int
	parts   []*raftcmdpb.Response
	mergeFn func([][]byte, ...*raftcmdpb.Response) *raftcmdpb.Response
	args    [][]byte
}

func newAggregationReq(n int, mergeFn func([][]byte, ...*raftcmdpb.Response) *raftcmdpb.Response, args [][]byte) *aggregationReq {
	return &aggregationReq{
		reply:   n,
		parts:   make([]*raftcmdpb.Response, n, n),
		mergeFn: mergeFn,
		args:    args,
	}
}

func (req *aggregationReq) addPart(index int, rsp *raftcmdpb.Response) bool {
	req.parts[index] = rsp
	req.reply--

	return req.reply == 0
}

func (req *aggregationReq) merge() *raftcmdpb.Response {
	return req.mergeFn(req.args, req.parts...)
}

func isAggregationPart(id []byte) bool {
	return len(id) > 16
}

func parseAggregationPart(id []byte) ([]byte, int) {
	return id[0:16], parseStrInt64(id[16:])
}

func parseStrInt64(data []byte) int {
	v, _ := strconv.ParseInt(string(data), 10, 64)
	return int(v)
}
