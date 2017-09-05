package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
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

type proposeBatch struct {
	pr       *PeerReplicate
	lastType int
	cmds     []*cmd
	complete bool
}

func newBatch(pr *PeerReplicate) *proposeBatch {
	return &proposeBatch{
		pr:       pr,
		complete: true,
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

func (b *proposeBatch) isLastComplete() bool {
	return b.complete
}

func (b *proposeBatch) startBatch() {
	b.complete = false
}

func (b *proposeBatch) completeBatch() {
	b.complete = true
}

func (b *proposeBatch) size() int {
	return len(b.cmds)
}

func (b *proposeBatch) isEmpty() bool {
	return 0 == b.size()
}

func (b *proposeBatch) isFull(lastSize int) bool {
	return globalCfg.RaftProposeBatchLimit == lastSize
}

func (b *proposeBatch) pop() *cmd {
	if b.isEmpty() {
		return nil
	}

	value := b.cmds[0]
	b.cmds[0] = nil
	b.cmds = b.cmds[1:]

	queueGauge.WithLabelValues(labelQueueBatch).Set(float64(len(value.req.Requests)))
	return value
}

func (b *proposeBatch) push(c *reqCtx) {
	adminReq := c.admin
	req := c.req
	cb := c.cb

	tp := b.getType(c)

	// use data key to store
	if tp != admin {
		key := req.Cmd[1]
		req.Cmd[1] = getDataKey(key)
	}

	last := b.lastCmd()
	if last == nil ||
		tp == admin || // admin request must in a single batch
		b.lastType != tp ||
		b.isFull(len(last.req.Requests)) {

		cell := b.pr.getCell()
		raftCMD := new(raftcmdpb.RaftCMDRequest)
		raftCMD.Header = &raftcmdpb.RaftRequestHeader{
			CellId:     cell.ID,
			Peer:       b.pr.getPeer(),
			ReadQuorum: true,
			UUID:       uuid.NewV4().Bytes(),
			CellEpoch:  cell.Epoch,
		}

		if tp == admin {
			raftCMD.AdminRequest = adminReq
		} else {
			raftCMD.Requests = append(raftCMD.Requests, req)
			log.Debugf("req: add to new batch. uuid=<%d>", req.UUID)
		}

		b.cmds = append(b.cmds, newCMD(raftCMD, cb))
	} else {
		if tp == admin {
			log.Fatal("bug: admin request must in a single batch")
		} else {
			last.req.Requests = append(last.req.Requests, req)
			if log.DebugEnabled() {
				log.Debugf("req: add to exists batch. uuid=<%d>, batch size=<%d>",
					req.UUID,
					len(last.req.Requests))
			}
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
