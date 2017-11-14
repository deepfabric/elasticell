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

package codec

import (
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/fagongzi/goetty"
	. "github.com/pingcap/check"
)

var _ = Suite(&testProxyCodecSuite{})

type testProxyCodecSuite struct {
}

func (s *testProxyCodecSuite) SetUpSuite(c *C) {

}

func (s *testProxyCodecSuite) TearDownSuite(c *C) {

}

func (s *testProxyCodecSuite) TestCodec(c *C) {
	buf := goetty.NewByteBuf(32)
	encoder := new(ProxyEncoder)
	decoder := new(ProxyDecoder)

	nt := &pdpb.WatcherNotify{
		Offset: 1,
	}
	c.Assert(encoder.Encode(nt, buf), IsNil)
	complete, msg, err := decoder.Decode(buf)
	c.Assert(complete, IsTrue)
	c.Assert(err, IsNil)
	_, ok := msg.(*pdpb.WatcherNotify)
	c.Assert(ok, IsTrue)

	buf.Clear()
	ntSync := &pdpb.WatcherNotifySync{
		Offset: 1,
	}
	c.Assert(encoder.Encode(ntSync, buf), IsNil)
	complete, msg, err = decoder.Decode(buf)
	c.Assert(complete, IsTrue)
	c.Assert(err, IsNil)
	_, ok = msg.(*pdpb.WatcherNotifySync)
	c.Assert(ok, IsTrue)

	buf.Clear()
	ntRsp := &pdpb.WatcherNotifyRsp{
		Offset: 1,
	}
	c.Assert(encoder.Encode(ntRsp, buf), IsNil)
	complete, msg, err = decoder.Decode(buf)
	c.Assert(complete, IsTrue)
	c.Assert(err, IsNil)
	_, ok = msg.(*pdpb.WatcherNotifySync)
	c.Assert(ok, IsTrue)
}
