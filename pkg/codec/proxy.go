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
	"fmt"
	"log"

	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

const (
	// RedisBegin tag for redis command
	RedisBegin = 0x01
	// WatcherNotifyBegin tag for notify
	WatcherNotifyBegin = 0x02
	// WatcherNotifySyncBegin tag for notify sync
	WatcherNotifySyncBegin = 0x03
	// WatcherNotifyRspBegin tag for notify rsp
	WatcherNotifyRspBegin = 0x04
)

// ProxyDecoder proxy decoder base on goetty
type ProxyDecoder struct {
}

// ProxyEncoder proxy encoder base on goetty
type ProxyEncoder struct {
}

// Decode return a decoded msg or wait for next tcp packet
func (decoder *ProxyDecoder) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	for {
		// remember the begin read index,
		// if we found has no enough data, we will resume this read index,
		// and waiting for next.
		backupReaderIndex := in.GetReaderIndex()

		c, err := in.ReadByte()
		if err != nil {
			return false, nil, err
		}

		if c == RedisBegin {
			if ok, size := hasEnoughData(in, backupReaderIndex); ok {
				return readRedis(in, size)
			}
			return false, nil, nil
		} else if c == WatcherNotifyBegin {
			if ok, size := hasEnoughData(in, backupReaderIndex); ok {
				return readPB(in, size, new(pdpb.WatcherNotify))
			}
			return false, nil, nil
		} else if c == WatcherNotifySyncBegin {
			if ok, size := hasEnoughData(in, backupReaderIndex); ok {
				return readPB(in, size, new(pdpb.WatcherNotifySync))
			}
			return false, nil, nil
		} else if c == WatcherNotifyRspBegin {
			if ok, size := hasEnoughData(in, backupReaderIndex); ok {
				return readPB(in, size, new(pdpb.WatcherNotifyRsp))
			}
			return false, nil, nil
		}

		return false, nil, fmt.Errorf("Error packet data with start char: %c", c)
	}
}

// Encode encode proxy message
func (encoder *ProxyEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	if msg, ok := data.(*raftcmdpb.Request); ok {
		return WriteProxyMessage(RedisBegin, msg, out)
	} else if msg, ok := data.(*pdpb.WatcherNotify); ok {
		return WriteProxyMessage(WatcherNotifyBegin, msg, out)
	} else if msg, ok := data.(*pdpb.WatcherNotifySync); ok {
		return WriteProxyMessage(WatcherNotifySyncBegin, msg, out)
	} else if msg, ok := data.(*pdpb.WatcherNotifyRsp); ok {
		return WriteProxyMessage(WatcherNotifyRspBegin, msg, out)
	}

	return fmt.Errorf("not support message: %v", data)
}

// WriteProxyMessage write a proxy to the buf
func WriteProxyMessage(tag byte, req util.Marashal, out *goetty.ByteBuf) error {
	value, err := req.Marshal()
	if err != nil {
		log.Fatalf("bug: marshal error, req=<%d> errors:%+v\n", req, err)
	}

	out.WriteByte(tag)
	out.WriteInt(len(value))
	out.Write(value)

	return nil
}

func hasEnoughData(in *goetty.ByteBuf, backupReaderIndex int) (bool, int) {
	if in.Readable() < 4 {
		in.SetReaderIndex(backupReaderIndex)
		return false, 0
	}

	size, _ := in.PeekInt(0)
	if in.Readable() < 4+size {
		in.SetReaderIndex(backupReaderIndex)
		return false, 0
	}

	in.Skip(4)
	return true, size
}

func readRedis(in *goetty.ByteBuf, size int) (bool, interface{}, error) {
	n, data, err := in.ReadBytes(size)
	if err != nil {
		return false, nil, err
	}

	if n != size {
		return false, nil, fmt.Errorf("read bytes not match length field, expect=<%d>, read=<%d>", size, n)
	}

	resp := new(raftcmdpb.Response)
	util.MustUnmarshal(resp, data)
	return true, resp, nil
}

func readPB(in *goetty.ByteBuf, size int, pb util.Unmarshal) (bool, interface{}, error) {
	n, data, err := in.ReadBytes(size)
	if err != nil {
		return false, nil, err
	}

	if n != size {
		return false, nil, fmt.Errorf("read bytes not match length field, expect=<%d>, read=<%d>", size, n)
	}

	util.MustUnmarshal(pb, data)
	return true, pb, nil
}
