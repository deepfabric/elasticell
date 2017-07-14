package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/deepfabric/elasticell/pkg/util"
)

func (s *Store) execZAdd(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	score, err := util.StrFloat64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getZSetEngine().ZAdd(args[0], score, args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
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

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZCard(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 1 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZCard(args[0])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZCount(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZCount(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZIncrBy(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	by, err := util.StrFloat64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getZSetEngine().ZIncrBy(args[0], args[1], by)
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

func (s *Store) execZLexCount(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZLexCount(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZRange(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	start, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	stop, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	withScores := len(args) == 4
	value, err := s.getZSetEngine().ZRange(args[0], start, stop)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		ScorePairArrayResult:         value,
		HasEmptyScorePairArrayResult: &withScores,
		Withscores:                   &withScores,
	}
}

func (s *Store) execZRangeByLex(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZRangeByLex(args[0], args[1], args[2])
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

func (s *Store) execZRangeByScore(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZRangeByScore(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	has := true
	return &raftcmdpb.Response{
		ScorePairArrayResult:         value,
		HasEmptyScorePairArrayResult: &has,
	}
}

func (s *Store) execZRank(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZRank(args[0], args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	if value < 0 {
		has := true
		return &raftcmdpb.Response{
			BulkResult:         nil,
			HasEmptyBulkResult: &has,
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZRem(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) < 2 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZRem(args[0], args[1:]...)
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

		ctx.metrics.sizeDiffHint -= size
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZRemRangeByLex(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZRemRangeByLex(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZRemRangeByRank(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	start, err := util.StrInt64(args[1])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	stop, err := util.StrInt64(args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	value, err := s.getZSetEngine().ZRemRangeByRank(args[0], start, stop)
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZRemRangeByScore(ctx *execContext, req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 3 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZRemRangeByScore(args[0], args[1], args[2])
	if err != nil {
		return &raftcmdpb.Response{
			ErrorResult: util.StringToSlice(err.Error()),
		}
	}

	return &raftcmdpb.Response{
		IntegerResult: &value,
	}
}

func (s *Store) execZScore(req *raftcmdpb.Request) *raftcmdpb.Response {
	cmd := redis.Command(req.Cmd)
	args := cmd.Args()

	if len(args) != 2 {
		return &raftcmdpb.Response{ErrorResult: redis.ErrInvalidCommandResp}
	}

	value, err := s.getZSetEngine().ZScore(args[0], args[1])
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
