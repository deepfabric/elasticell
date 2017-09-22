package raftstore

import (
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/util"
	. "github.com/pingcap/check"
)

type transportTestSuite struct {
	from *transport
	to   *transport

	fromAddr     string
	toAddr       string
	fromID, toID uint64
}

func (s *transportTestSuite) SetUpSuite(c *C) {
	s.fromAddr = "127.0.0.1:12345"
	s.toAddr = "127.0.0.1:22345"

	s.fromID = 1
	s.toID = 2

	s1 := new(Store)
	s1.id = s.fromID
	globalCfg = newTestStoreCfg(s.fromAddr)
	s1.runner = util.NewRunner()
	s.from = newTransport(s1, nil, nil)
	s.from.getStoreAddrFun = s.parse
	go s.from.start()
	<-s.from.server.Started()

	s2 := new(Store)
	s2.id = s.toID
	globalCfg = newTestStoreCfg(s.toAddr)
	s2.runner = util.NewRunner()
	s.to = newTransport(s2, nil, nil)
	s.to.getStoreAddrFun = s.parse

	go s.to.start()
	<-s.to.server.Started()
}

func (s *transportTestSuite) TearDownSuite(c *C) {
	s.from.stop()
	s.to.stop()
}

func (s *transportTestSuite) TestSend(c *C) {
	ch := make(chan interface{})
	defer close(ch)

	msg := &mraft.RaftMessage{}
	msg.ToPeer.StoreID = s.toID
	s.from.send(msg)

	s.to.handler = func(msg interface{}) {
		ch <- msg
	}

	select {
	case msg := <-ch:
		_, ok := msg.(*mraft.RaftMessage)
		c.Assert(ok, IsTrue)
	case <-time.After(time.Second):
		c.Fail()
	}
}

func (s *transportTestSuite) parse(storeID uint64) (string, error) {
	if storeID == s.fromID {
		return s.fromAddr, nil
	} else if storeID == s.toID {
		return s.toAddr, nil
	}

	return "", nil
}

func newTestStoreCfg(addr string) *Cfg {
	c := new(Cfg)
	c.StoreAddr = addr
	c.StoreAdvertiseAddr = c.StoreAddr
	c.Raft = new(RaftCfg)
	c.Raft.ElectionTick = 2
	c.Raft.BaseTick = 1000
	c.Raft.HeartbeatTick = 1
	c.Raft.MaxSizePerMsg = 1024 * 1024
	c.Raft.MaxSizePerEntry = 8 * 1024 * 1024
	c.Raft.MaxInflightMsgs = 256
	c.RaftMessageSendBatchLimit = 10
	c.RaftMessageWorkerCount = 1
	return c
}
