package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util/uuid"
)

const (
	read = iota
	write
)

type reqCtx struct {
	req *raftcmdpb.Request
	cb  func(*raftcmdpb.RaftCMDResponse)
}

type proposeBatch struct {
	pr              *PeerReplicate
	lastType        int
	cmds            []*cmd
	preEntriesSaved bool
}

func newBatch(pr *PeerReplicate) *proposeBatch {
	return &proposeBatch{
		pr:              pr,
		preEntriesSaved: true,
	}
}

func (b *proposeBatch) getType(req *raftcmdpb.Request) int {
	if b.pr.isWrite(req) {
		return write
	}

	return read
}

func (b *proposeBatch) isPreEntriesSaved() bool {
	return b.preEntriesSaved
}

func (b *proposeBatch) prePropose() {
	b.preEntriesSaved = false
}

func (b *proposeBatch) postPropose() {
	b.preEntriesSaved = true
}

func (b *proposeBatch) hasCMD() bool {
	return b.size() > 0
}

func (b *proposeBatch) size() int {
	return len(b.cmds)
}

func (b *proposeBatch) isEmpty() bool {
	return 0 == b.size()
}

func (b *proposeBatch) isFull() bool {
	return globalCfg.RaftProposeBatchLimit == b.size()
}

func (b *proposeBatch) pop() *cmd {
	if b.isEmpty() {
		return nil
	}

	value := b.cmds[0]
	b.cmds[0] = nil
	b.cmds = b.cmds[1:]

	requestBatchSizeGauge.Set(float64(len(value.req.Requests)))
	return value
}

func (b *proposeBatch) push(c *reqCtx) {
	req := c.req
	cb := c.cb

	tp := b.getType(req)
	key := req.Cmd[1]
	req.Cmd[1] = getDataKey(key)

	last := b.lastCmd()
	if last == nil || b.lastType != tp || b.isFull() {
		cell := b.pr.getCell()
		raftCMD := new(raftcmdpb.RaftCMDRequest)
		raftCMD.Header = &raftcmdpb.RaftRequestHeader{
			CellId:     cell.ID,
			Peer:       b.pr.getPeer(),
			ReadQuorum: true,
			UUID:       uuid.NewV4().Bytes(),
			CellEpoch:  cell.Epoch,
		}

		raftCMD.Requests = append(raftCMD.Requests, req)
		b.cmds = append(b.cmds, newCMD(raftCMD, cb))
	} else {
		last.req.Requests = append(last.req.Requests, req)
	}

	b.lastType = tp
}

func (b *proposeBatch) lastCmd() *cmd {
	if b.isEmpty() {
		return nil
	}

	return b.cmds[b.size()-1]
}
