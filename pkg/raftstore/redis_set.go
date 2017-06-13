package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
)

func (s *Store) execSAdd(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getSetEngine().SAdd(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
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

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execSRem(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getSetEngine().SRem(args[0], args[1:]...)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value == 0 {
		ctx.metrics.deleteKeysHint++
	}

	var size int64
	for _, arg := range args[1:] {
		size += int64(len(arg))
	}

	ctx.metrics.sizeDiffHint -= size

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execSCard(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getSetEngine().SCard(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execSMembers(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getSetEngine().SMembers(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		SliceArrayResult:         value,
		HasEmptySliceArrayResult: &has,
	}
}

func (s *Store) execSIsMember(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getSetEngine().SIsMember(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execSPop(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return redis.ErrInvalidCommandResp
	}

	value, err := s.getSetEngine().SPop(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		BulkResult:         value,
		HasEmptyBulkResult: &has,
	}
}
