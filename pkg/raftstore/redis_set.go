package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
)

func (s *Store) execSAdd(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getSetEngine().SAdd(args[0], args[1:]...)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	if value > 0 {
		var size int64
		for _, arg := range args[1:] {
			size += int64(len(arg))
		}

		if value == 1 {
			size += int64(len(args[0]))
			ctx.metrics.writtenKeys++
		}

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execSRem(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getSetEngine().SRem(args[0], args[1:]...)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	if value == 0 {
		ctx.metrics.deleteKeysHint++
	}

	var size int64
	for _, arg := range args[1:] {
		size += int64(len(arg))
	}

	ctx.metrics.sizeDiffHint -= size

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execSCard(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getSetEngine().SCard(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp

}

func (s *Store) execSMembers(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getSetEngine().SMembers(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	has := true
	rsp := pool.AcquireResponse()
	rsp.SliceArrayResult = value
	rsp.HasEmptySliceArrayResult = &has
	return rsp
}

func (s *Store) execSIsMember(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getSetEngine().SIsMember(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execSPop(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getSetEngine().SPop(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	has := true
	rsp := pool.AcquireResponse()
	rsp.BulkResult = value
	rsp.HasEmptyBulkResult = &has
	return rsp
}
