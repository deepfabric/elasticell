package raftstore

import (
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/fagongzi/goetty"
)

var (
	reqCtxPool           sync.Pool
	cmdPool              sync.Pool
	readyContextPool     sync.Pool
	asyncApplyResultPool sync.Pool
	applyContextPool     sync.Pool
	entryPool            sync.Pool
	bufPool              sync.Pool
	redisBatchPool       sync.Pool
)

var (
	emptyRaftState    = mraft.RaftLocalState{}
	emptyApplyState   = mraft.RaftApplyState{}
	emptyApplyMetrics = applyMetrics{}
)

func acquireRedisBatch() *redisBatch {
	v := redisBatchPool.Get()
	if v == nil {
		return &redisBatch{}
	}

	return v.(*redisBatch)
}

func releaseRedisBatch(batch *redisBatch) {
	batch.reset()
	redisBatchPool.Put(batch)
}

func acquireBuf() *goetty.ByteBuf {
	v := bufPool.Get()
	if v == nil {
		return goetty.NewByteBuf(64)
	}

	buf := v.(*goetty.ByteBuf)
	buf.Resume(64)

	return buf
}

func releaseBuf(buf *goetty.ByteBuf) {
	buf.Clear()
	buf.Release()
	bufPool.Put(buf)
}

func acquireEntry() *raftpb.Entry {
	v := entryPool.Get()
	if v == nil {
		return &raftpb.Entry{}
	}

	return v.(*raftpb.Entry)
}

func releaseEntry(ent *raftpb.Entry) {
	ent.Reset()
	entryPool.Put(ent)
}

func acquireReqCtx() *reqCtx {
	v := reqCtxPool.Get()
	if v == nil {
		return &reqCtx{}
	}

	return v.(*reqCtx)
}

func releaseReqCtx(req *reqCtx) {
	req.reset()
	reqCtxPool.Put(req)
}

func acquireCmd() *cmd {
	v := cmdPool.Get()
	if v == nil {
		return &cmd{}
	}

	return v.(*cmd)
}

func releaseCmd(c *cmd) {
	c.reset()
	cmdPool.Put(c)
}

func acquireReadyContext() *readyContext {
	v := readyContextPool.Get()
	if v == nil {
		return &readyContext{}
	}

	return v.(*readyContext)
}

func releaseReadyContext(ctx *readyContext) {
	ctx.reset()
	readyContextPool.Put(ctx)
}

func acquireAsyncApplyResult() *asyncApplyResult {
	v := asyncApplyResultPool.Get()
	if v == nil {
		return &asyncApplyResult{}
	}

	return v.(*asyncApplyResult)
}

func releaseAsyncApplyResult(res *asyncApplyResult) {
	res.reset()
	asyncApplyResultPool.Put(res)
}

func acquireApplyContext() *applyContext {
	v := applyContextPool.Get()
	if v == nil {
		return &applyContext{}
	}

	return v.(*applyContext)
}

func releaseApplyContext(ctx *applyContext) {
	ctx.reset()
	applyContextPool.Put(ctx)
}
