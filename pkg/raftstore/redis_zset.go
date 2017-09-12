package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
)

func (s *Store) execZAdd(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	score, err := util.StrFloat64(args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getZSetEngine().ZAdd(args[0], score, args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	if value > 0 {
		var size int64

		if value == 1 {
			ctx.metrics.writtenKeys++
			size += int64(len(args[0]))
		}

		size += int64(len(args[2]))
		size += int64(len(args[1]))

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZCard(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZCard(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZCount(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZCount(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZIncrBy(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	by, err := util.StrFloat64(args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getZSetEngine().ZIncrBy(args[0], args[1], by)
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

func (s *Store) execZLexCount(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZLexCount(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZRange(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	start, err := util.StrInt64(args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	stop, err := util.StrInt64(args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	withScores := len(args) == 4
	value, err := s.getZSetEngine().ZRange(args[0], start, stop)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.ScorePairArrayResult = value
	rsp.HasEmptyScorePairArrayResult = &withScores
	rsp.Withscores = &withScores
	return rsp
}

func (s *Store) execZRangeByLex(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZRangeByLex(args[0], args[1], args[2])
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

func (s *Store) execZRangeByScore(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZRangeByScore(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	has := true
	rsp := pool.AcquireResponse()
	rsp.ScorePairArrayResult = value
	rsp.HasEmptyScorePairArrayResult = &has
	return rsp
}

func (s *Store) execZRank(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZRank(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	if value < 0 {
		has := true
		rsp := pool.AcquireResponse()
		rsp.BulkResult = nil
		rsp.HasEmptyBulkResult = &has
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZRem(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZRem(args[0], args[1:]...)
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

		ctx.metrics.sizeDiffHint -= size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZRemRangeByLex(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZRemRangeByLex(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZRemRangeByRank(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	start, err := util.StrInt64(args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	stop, err := util.StrInt64(args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getZSetEngine().ZRemRangeByRank(args[0], start, stop)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZRemRangeByScore(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZRemRangeByScore(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = util.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZScore(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine().ZScore(args[0], args[1])
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
