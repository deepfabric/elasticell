package proxy

import (
	"bytes"
	"strings"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/pilosa/pilosa/roaring"
)

const (
	optionWithBM = "WITHBM"
)

// bmand [withbm|start count]  bm1 bm2 [bm3 bm4]
func (p *RedisProxy) doBMAnd(rs *redisSession, cmd redis.Command) (bool, error) {
	return p.doBMAggregation(rs, cmd, p.doBMAndMerge)
}

// bmor [withbm|start count]  bm1 bm2 [bm3 bm4]
func (p *RedisProxy) doBMOr(rs *redisSession, cmd redis.Command) (bool, error) {
	return p.doBMAggregation(rs, cmd, p.doBMOrMerge)
}

// bmxor [withbm|start count]  bm1 bm2 [bm3 bm4]
func (p *RedisProxy) doBMXor(rs *redisSession, cmd redis.Command) (bool, error) {
	return p.doBMAggregation(rs, cmd, p.doBMXorMerge)
}

// bmandnot [withbm|start count]  bm1 bm2 [bm3 bm4]
func (p *RedisProxy) doBMAndNot(rs *redisSession, cmd redis.Command) (bool, error) {
	return p.doBMAggregation(rs, cmd, p.doBMAndNotMerge)
}

func (p *RedisProxy) doBMAggregation(rs *redisSession, cmd redis.Command, mergeFn func([][]byte, ...*raftcmdpb.Response) *raftcmdpb.Response) (bool, error) {
	offset := 1
	n := len(cmd.Args())
	if n < 3 {
		return false, errInvalidCommand
	}

	if strings.ToUpper(hack.SliceToString(cmd.Args()[0])) != optionWithBM {
		if n < 4 {
			return false, errInvalidCommand
		}

		offset = 2
	}

	id := newID()
	rs.addAggregation(id, newAggregationReq(n-offset, mergeFn, cmd.Args()[0:offset]))

	for idx, key := range cmd.Args()[offset:] {
		cmd := redis.Command([][]byte{cmdGet, key})
		p.addToForward(newReqUUID(append(id, format.UInt64ToString(uint64(idx))...), cmd, rs))
	}

	return true, nil
}

func (p *RedisProxy) doBMAndMerge(args [][]byte, rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	bm := roaring.NewBTreeBitmap()
	tmp := roaring.NewBTreeBitmap()
	var target *roaring.Bitmap
	for idx, rsp := range rsps {
		if len(rsp.BulkResult) > 0 {
			tmp.Containers.Reset()

			if idx == 0 {
				target = bm
			} else {
				target = tmp
			}

			_, _, err := target.ImportRoaringBits(rsp.BulkResult, false, false, 0)
			if err != nil {
				return &raftcmdpb.Response{
					ErrorResult: hack.StringToSlice(err.Error()),
				}
			}

			if idx > 0 {
				bm = bm.Intersect(tmp)
			}
		}
	}

	return p.buildResult(bm, args)
}

func (p *RedisProxy) doBMOrMerge(args [][]byte, rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	bm := roaring.NewBTreeBitmap()
	tmp := roaring.NewBTreeBitmap()
	var target *roaring.Bitmap
	for idx, rsp := range rsps {
		if len(rsp.BulkResult) > 0 {
			tmp.Containers.Reset()

			if idx == 0 {
				target = bm
			} else {
				target = tmp
			}

			_, _, err := target.ImportRoaringBits(rsp.BulkResult, false, false, 0)
			if err != nil {
				return &raftcmdpb.Response{
					ErrorResult: hack.StringToSlice(err.Error()),
				}
			}

			if idx > 0 {
				bm = bm.Union(tmp)
			}
		}
	}

	return p.buildResult(bm, args)
}

func (p *RedisProxy) doBMXorMerge(args [][]byte, rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	bm := roaring.NewBTreeBitmap()
	tmp := roaring.NewBTreeBitmap()
	var target *roaring.Bitmap
	for idx, rsp := range rsps {
		if len(rsp.BulkResult) > 0 {
			tmp.Containers.Reset()

			if idx == 0 {
				target = bm
			} else {
				target = tmp
			}

			_, _, err := target.ImportRoaringBits(rsp.BulkResult, false, false, 0)
			if err != nil {
				return &raftcmdpb.Response{
					ErrorResult: hack.StringToSlice(err.Error()),
				}
			}

			if idx > 0 {
				bm = bm.Xor(tmp)
			}
		}
	}

	return p.buildResult(bm, args)
}

func (p *RedisProxy) doBMAndNotMerge(args [][]byte, rsps ...*raftcmdpb.Response) *raftcmdpb.Response {
	targets := make([]*roaring.Bitmap, 0, len(rsps))
	for _, rsp := range rsps {
		if len(rsp.BulkResult) > 0 {
			bm := roaring.NewBTreeBitmap()
			targets = append(targets, bm)
			_, _, err := bm.ImportRoaringBits(rsp.BulkResult, false, false, 0)
			if err != nil {
				return &raftcmdpb.Response{
					ErrorResult: hack.StringToSlice(err.Error()),
				}
			}
		}
	}

	union := targets[0]
	and := targets[0]

	for _, bm := range targets[1:] {
		union = union.Union(bm)
		and = and.Intersect(bm)
	}

	return p.buildResult(union.Xor(and), args)
}

func (p *RedisProxy) buildResult(bm *roaring.Bitmap, args [][]byte) *raftcmdpb.Response {
	log.Debugf("bm and args: %+v", args)
	if len(args) < 2 {
		buf := bytes.NewBuffer(nil)
		bm.WriteTo(buf)
		return &raftcmdpb.Response{
			BulkResult: buf.Bytes(),
		}
	}

	start, err := format.ParseStrUInt64(hack.SliceToString(args[0]))
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: hack.StringToSlice(err.Error()),
		}
	}

	limit, err := format.ParseStrUInt64(hack.SliceToString(args[1]))
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: hack.StringToSlice(err.Error()),
		}
	}

	rsp := &raftcmdpb.Response{}
	var values [][]byte
	count := uint64(0)
	itr := bm.Iterator()
	itr.Seek(start)
	for {
		value, eof := itr.Next()
		if eof {
			break
		}

		values = append(values, format.UInt64ToString(value))
		count++

		if count >= limit {
			break
		}
	}
	rsp.SliceArrayResult = values
	rsp.HasEmptySliceArrayResult = len(values) == 0
	return rsp
}
