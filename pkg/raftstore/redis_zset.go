package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
)

func (s *Store) execZAdd(ctx *applyContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	score, err := format.ParseStrFloat64(hack.SliceToString(args[1]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getZSetEngine(ctx.req.Header.CellId).ZAdd(args[0], score, args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	if value > 0 {
		var size uint64

		if value == 1 {
			ctx.metrics.writtenKeys++
			size += uint64(len(args[0]))
		}

		size += uint64(len(args[2]))
		size += uint64(len(args[1]))

		ctx.metrics.writtenBytes += size
		ctx.metrics.sizeDiffHint += size
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZCard(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZCard(args[0])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZCount(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZCount(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
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

	by, err := format.ParseStrFloat64(hack.SliceToString(args[2]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getZSetEngine(ctx.req.Header.CellId).ZIncrBy(args[0], args[1], by)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.BulkResult = value
	rsp.HasEmptyBulkResult = len(value) == 0
	return rsp
}

func (s *Store) execZLexCount(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZLexCount(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZRange(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	start, err := format.ParseStrInt64(hack.SliceToString(args[1]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	stop, err := format.ParseStrInt64(hack.SliceToString(args[2]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	withScores := len(args) == 4
	value, err := s.getZSetEngine(id).ZRange(args[0], start, stop)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.ScorePairArrayResult = value
	rsp.HasEmptyScorePairArrayResult = len(value) == 0
	rsp.Withscores = withScores
	return rsp
}

func (s *Store) execZRangeByLex(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZRangeByLex(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.SliceArrayResult = value
	rsp.HasEmptySliceArrayResult = len(value) == 0
	return rsp
}

func (s *Store) execZRangeByScore(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZRangeByScore(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.ScorePairArrayResult = value
	rsp.HasEmptyScorePairArrayResult = len(value) == 0
	rsp.Withscores = len(args) >= 4
	return rsp
}

func (s *Store) execZRank(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZRank(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	if value < 0 {
		rsp := pool.AcquireResponse()
		rsp.BulkResult = nil
		rsp.HasEmptyBulkResult = true
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

	value, err := s.getZSetEngine(ctx.req.Header.CellId).ZRem(args[0], args[1:]...)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	if value > 0 {
		var size uint64

		for _, arg := range args[1:] {
			size += uint64(len(arg))
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

	value, err := s.getZSetEngine(ctx.req.Header.CellId).ZRemRangeByLex(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
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

	start, err := format.ParseStrInt64(hack.SliceToString(args[1]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	stop, err := format.ParseStrInt64(hack.SliceToString(args[2]))
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	value, err := s.getZSetEngine(ctx.req.Header.CellId).ZRemRangeByRank(args[0], start, stop)
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
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

	value, err := s.getZSetEngine(ctx.req.Header.CellId).ZRemRangeByScore(args[0], args[1], args[2])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.IntegerResult = &value
	return rsp
}

func (s *Store) execZScore(id uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp

		return rsp
	}

	value, err := s.getZSetEngine(id).ZScore(args[0], args[1])
	if err != nil {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = hack.StringToSlice(err.Error())
		return rsp
	}

	rsp := pool.AcquireResponse()
	rsp.BulkResult = value
	rsp.HasEmptyBulkResult = len(value) == 0
	return rsp

}
