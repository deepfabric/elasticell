// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"testing"
	"time"

	"github.com/fagongzi/goetty"
	gredis "github.com/fagongzi/goetty/protocol/redis"
	"github.com/garyburd/redigo/redis"
	. "github.com/pingcap/check"
)

var _ = Suite(&testRedisSuite{})

func TestRedis(t *testing.T) {
	TestingT(t)
}

type testRedisSuite struct {
	svr *goetty.Server
}

func (s *testRedisSuite) SetUpSuite(c *C) {
	s.svr = goetty.NewServerSize(":6379",
		Decoder,
		Encoder,
		1024,
		1024,
		goetty.NewInt64IDGenerator())
	go s.svr.Start(s.doConnection)
	select {
	case <-s.svr.Started():
	case <-time.After(time.Second):
		c.Failed()
	}
}

func (s *testRedisSuite) TearDownSuite(c *C) {
	s.svr.Stop()
}

func (s *testRedisSuite) TestParser(c *C) {
	conn, err := redis.DialTimeout("tcp", ":6379", time.Second, time.Second, time.Second)
	c.Assert(err, IsNil)
	defer conn.Close()

	info, err := redis.String(conn.Do("set", "key1", "value1"))
	c.Assert(err, IsNil)
	c.Assert(info, Equals, "OK")

	info, err = redis.String(conn.Do("set", "key1", "value1"))
	c.Assert(err, IsNil)
	c.Assert(info, Equals, "OK")
}

func (s *testRedisSuite) doConnection(session goetty.IOSession) error {
	for {
		req, err := session.Read()
		if err != nil {
			return err
		}

		cmd := req.(Command)
		if cmd.CmdString() == "set" {
			gredis.WriteStatus([]byte("OK"), session.OutBuf())
			session.WriteOutBuf()
		}
	}
}
