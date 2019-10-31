package server

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/redis"
)

// command like: bmcreate bm1 [1, 3, 3]
func (s *RedisServer) onBMCreate(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: bmadd bm1 1 2 3 4 ...
func (s *RedisServer) onBMAdd(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: b,remove bm1 1 [2 3 4]
func (s *RedisServer) onBMRemove(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) < 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: bmclear bm1
func (s *RedisServer) onBMClear(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: bmdel bm1
func (s *RedisServer) onBMDel(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: bmrange bm1 start count
func (s *RedisServer) onBMRange(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 3 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: bmcount bm1
func (s *RedisServer) onBMCount(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 1 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}

// command like: bmcontains bm1 1
func (s *RedisServer) onBMContains(cmdType raftcmdpb.CMDType, cmd redis.Command, session *session) ([]byte, error) {
	args := cmd.Args()
	if len(args) != 2 {
		rsp := pool.AcquireResponse()
		rsp.ErrorResult = redis.ErrInvalidCommandResp
		session.onResp(rsp)
		return nil, nil
	}

	return s.store.OnRedisCommand(session.id, cmdType, cmd, s.onResp)
}
