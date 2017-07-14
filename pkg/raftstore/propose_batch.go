package raftstore

import (
	"sync"

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
	sync.RWMutex

	pr       *PeerReplicate
	lastType int
	cmds     []*cmd

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
	b.RLock()
	value := b.preEntriesSaved
	b.RUnlock()

	return value
}

func (b *proposeBatch) prePropose() {
	b.Lock()
	b.preEntriesSaved = false
	b.Unlock()
}

func (b *proposeBatch) postPropose() {
	b.Lock()
	b.preEntriesSaved = true
	b.Unlock()
}

func (b *proposeBatch) hasCMD() bool {
	b.RLock()
	value := b.size() > 0
	b.RUnlock()

	return value
}

func (b *proposeBatch) size() int {
	return len(b.cmds)
}

func (b *proposeBatch) isEmpty() bool {
	return 0 == b.size()
}

func (b *proposeBatch) pop() *cmd {
	b.RLock()
	if b.isEmpty() {
		b.RUnlock()
		return nil
	}

	value := b.cmds[0]
	b.cmds[0] = nil
	b.cmds = b.cmds[1:]
	b.RUnlock()

	return value
}

func (b *proposeBatch) push(c *reqCtx) {
	b.Lock()
	req := c.req
	cb := c.cb

	tp := b.getType(req)
	key := req.Cmd[1]
	req.Cmd[1] = getDataKey(key)

	last := b.lastCmd()
	if last == nil || b.lastType != tp {
		cell := b.pr.getCell()
		raftCMD := new(raftcmdpb.RaftCMDRequest)
		raftCMD.Header = &raftcmdpb.RaftRequestHeader{
			CellId:     cell.ID,
			Peer:       b.pr.getPeer(),
			ReadQuorum: true, // TODO: configuration
			UUID:       uuid.NewV4().Bytes(),
			CellEpoch:  cell.Epoch,
		}

		raftCMD.Requests = append(raftCMD.Requests, req)
		b.cmds = append(b.cmds, newCMD(raftCMD, cb))
	} else {
		last.req.Requests = append(last.req.Requests, req)
	}

	b.lastType = tp
	b.Unlock()
}

func (b *proposeBatch) lastCmd() *cmd {
	if b.isEmpty() {
		return nil
	}

	return b.cmds[b.size()-1]
}
