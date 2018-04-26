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
	"errors"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	gredis "github.com/fagongzi/goetty/protocol/redis"
	pe "github.com/pkg/errors"
)

var (
	// ErrIllegalPacket parse err
	ErrIllegalPacket = errors.New("illegal packet data")
)

const (
	// CMDBegin prefix of redis command
	CMDBegin = '*'
	// ProxyBegin 0x01
	ProxyBegin = 0x01

	defaultBufferSize = 64
)

func readCommand(in *goetty.ByteBuf) (bool, interface{}, error) {
	for {
		c, err := in.PeekByte(0)
		if err != nil {
			return false, nil, err
		}

		switch c {
		case CMDBegin:
			return gredis.ReadCommand(in)
		case ProxyBegin:
			return readCommandByProxyProtocol(in)
		default:
			return false, nil, pe.Wrap(ErrIllegalPacket, "")
		}
	}
}

func readCommandByProxyProtocol(in *goetty.ByteBuf) (bool, interface{}, error) {
	// remember the begin read index,
	// if we found has no enough data, we will resume this read index,
	// and waiting for next.
	backupReaderIndex := in.GetReaderIndex()

	in.Skip(1)

	if in.Readable() < 4 {
		in.SetReaderIndex(backupReaderIndex)
		return false, nil, nil
	}

	size, _ := in.PeekInt(0)
	if in.Readable() < 4+size {
		in.SetReaderIndex(backupReaderIndex)
		return false, nil, nil
	}

	in.Skip(4)
	n, data, err := in.ReadRawBytes(size)
	if err != nil {
		log.Fatal("bug: can't read failed")
	}

	if n != size {
		log.Fatal("bug: can't read mismatch")
	}

	req := pool.AcquireRequest()
	util.MustUnmarshal(req, data)
	in.Skip(size)
	return true, req, nil
}
