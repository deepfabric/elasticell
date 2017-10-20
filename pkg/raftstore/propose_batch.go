package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
	"github.com/fagongzi/goetty"
)

const (
	read = iota
	write
	admin
)

type reqCtx struct {
	admin *raftcmdpb.AdminRequest
	req   *raftcmdpb.Request
	cb    func(*raftcmdpb.RaftCMDResponse)
}

func (r *reqCtx) reset() {
	r.admin = nil
	r.cb = nil
	r.req = nil
}

type proposeBatch struct {
	pr *PeerReplicate

	buf      *goetty.ByteBuf
	lastType int
	cmds     []*cmd
}

func newBatch(pr *PeerReplicate) *proposeBatch {
	return &proposeBatch{
		pr:  pr,
		buf: goetty.NewByteBuf(512),
	}
}

func (b *proposeBatch) getType(c *reqCtx) int {
	if c.admin != nil {
		return admin
	}

	if b.pr.isWrite(c.req) {
		return write
	}

	return read
}

func (b *proposeBatch) size() int {
	return len(b.cmds)
}

func (b *proposeBatch) isEmpty() bool {
	return 0 == b.size()
}

func (b *proposeBatch) isFull(lastSize uint64) bool {
	return globalCfg.BatchSizeProposal == lastSize
}

func (b *proposeBatch) pop() *cmd {
	if b.isEmpty() {
		return nil
	}

	value := b.cmds[0]
	b.cmds[0] = nil
	b.cmds = b.cmds[1:]

	queueGauge.WithLabelValues(labelQueueBatchSize).Set(float64(len(value.req.Requests)))
	queueGauge.WithLabelValues(labelQueueBatch).Set(float64(b.size()))
	return value
}

func (b *proposeBatch) push(c *reqCtx) {
	adminReq := c.admin
	req := c.req
	cb := c.cb
	tp := b.getType(c)

	releaseReqCtx(c)

	isAdmin := tp == admin

	// use data key to store
	if !isAdmin {
		key := req.Cmd[1]
		req.Cmd[1] = getDataKey0(key, b.buf)
		b.buf.Clear()
	}

	last := b.lastCmd()
	if last == nil ||
		isAdmin || // admin request must in a single batch
		b.lastType != tp ||
		b.isFull(uint64(len(last.req.Requests))) {

		cell := b.pr.getCell()
		raftCMD := pool.AcquireRaftCMDRequest()
		raftCMD.Header = pool.AcquireRaftRequestHeader()
		raftCMD.Header.CellId = cell.ID
		raftCMD.Header.Peer = b.pr.getPeer()
		raftCMD.Header.ReadQuorum = true
		raftCMD.Header.UUID = uuid.NewV4().Bytes()
		raftCMD.Header.CellEpoch = cell.Epoch

		if isAdmin {
			raftCMD.AdminRequest = adminReq
		} else {
			raftCMD.Requests = append(raftCMD.Requests, req)
			if log.DebugEnabled() {
				log.Debugf("req: add to new batch. uuid=<%d>", req.UUID)
			}
		}

		b.cmds = append(b.cmds, newCMD(raftCMD, cb))
		queueGauge.WithLabelValues(labelQueueBatch).Set(float64(b.size()))
	} else {
		if isAdmin {
			log.Fatal("bug: admin request must in a single batch")
		}

		last.req.Requests = append(last.req.Requests, req)
		if log.DebugEnabled() {
			log.Debugf("req: add to exists batch. uuid=<%d>, batch size=<%d>",
				req.UUID,
				len(last.req.Requests))
		}
	}

	b.lastType = tp
}

func (b *proposeBatch) lastCmd() *cmd {
	if b.isEmpty() {
		return nil
	}

	return b.cmds[b.size()-1]
}
