package raftstore

import "github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"

func (s *Store) initRedisHandle() {
	s.redisWriteHandles[raftcmdpb.Set] = s.execKVSet
	s.redisWriteHandles[raftcmdpb.Incrby] = s.execKVIncrBy
	s.redisWriteHandles[raftcmdpb.Incr] = s.execKVIncr
	s.redisWriteHandles[raftcmdpb.Decrby] = s.execKVDecrby
	s.redisWriteHandles[raftcmdpb.Decr] = s.execKVDecr
	s.redisWriteHandles[raftcmdpb.GetSet] = s.execKVGetSet
	s.redisWriteHandles[raftcmdpb.Append] = s.execKVAppend
	s.redisWriteHandles[raftcmdpb.Setnx] = s.execKVSetNX
	s.redisWriteHandles[raftcmdpb.HSet] = s.execHSet
	s.redisWriteHandles[raftcmdpb.HDel] = s.execHDel
	s.redisWriteHandles[raftcmdpb.HMSet] = s.execHMSet
	s.redisWriteHandles[raftcmdpb.HSetNX] = s.execHSetNX
	s.redisWriteHandles[raftcmdpb.HIncrBy] = s.execHIncrBy
	s.redisWriteHandles[raftcmdpb.LInsert] = s.execLInsert
	s.redisWriteHandles[raftcmdpb.LPop] = s.execLPop
	s.redisWriteHandles[raftcmdpb.LPush] = s.execLPush
	s.redisWriteHandles[raftcmdpb.LPushX] = s.execLPushX
	s.redisWriteHandles[raftcmdpb.LRem] = s.execLRem
	s.redisWriteHandles[raftcmdpb.LSet] = s.execLSet
	s.redisWriteHandles[raftcmdpb.LTrim] = s.execLTrim
	s.redisWriteHandles[raftcmdpb.RPop] = s.execRPop
	s.redisWriteHandles[raftcmdpb.RPush] = s.execRPush
	s.redisWriteHandles[raftcmdpb.RPushX] = s.execRPushX
	s.redisWriteHandles[raftcmdpb.SAdd] = s.execSAdd
	s.redisWriteHandles[raftcmdpb.SRem] = s.execSRem

	s.redisReadHandles[raftcmdpb.Get] = s.execKVGet
	s.redisReadHandles[raftcmdpb.StrLen] = s.execKVStrLen
	s.redisReadHandles[raftcmdpb.HGet] = s.execHGet
	s.redisReadHandles[raftcmdpb.HExists] = s.execHExists
	s.redisReadHandles[raftcmdpb.HKeys] = s.execHKeys
	s.redisReadHandles[raftcmdpb.HVals] = s.execHVals
	s.redisReadHandles[raftcmdpb.HGetAll] = s.execHGetAll
	s.redisReadHandles[raftcmdpb.HLen] = s.execHLen
	s.redisReadHandles[raftcmdpb.HMGet] = s.execHMGet
	s.redisReadHandles[raftcmdpb.HStrLen] = s.execHStrLen
	s.redisReadHandles[raftcmdpb.LIndex] = s.execLIndex
	s.redisReadHandles[raftcmdpb.LLEN] = s.execLLEN
	s.redisReadHandles[raftcmdpb.LRange] = s.execLRange
	s.redisReadHandles[raftcmdpb.SCard] = s.execSCard
	s.redisReadHandles[raftcmdpb.SIsMember] = s.execSIsMember
	s.redisReadHandles[raftcmdpb.SMembers] = s.execSMembers
}
