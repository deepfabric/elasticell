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
	"strconv"

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
			return readCommandByRedisProtocol(in)
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

func readCommandByRedisProtocol(in *goetty.ByteBuf) (bool, interface{}, error) {
	// remember the begin read index,
	// if we found has no enough data, we will resume this read index,
	// and waiting for next.
	backupReaderIndex := in.GetReaderIndex()

	// 1. Read ( *<number of arguments> CR LF )
	in.Skip(1)

	// 2. Read number of arguments
	count, argsCount, err := readStringInt(in)
	if count == 0 && err == nil {
		in.SetReaderIndex(backupReaderIndex)
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	data := make([][]byte, argsCount)

	// 3. Read args
	for i := 0; i < argsCount; i++ {
		// 3.1 Read ( $<number of bytes of argument 1> CR LF )
		c, err := in.ReadByte()
		if err != nil {
			return false, nil, err
		}

		// 3.2 Read ( *<number of arguments> CR LF )

		if c != gredis.ARGBegin {
			return false, nil, pe.Wrapf(ErrIllegalPacket, "")
		}

		count, argBytesCount, err := readStringInt(in)
		if count == 0 && err == nil {
			in.SetReaderIndex(backupReaderIndex)
			return false, nil, nil
		} else if err != nil {
			return false, nil, err
		} else if count < 2 {
			return false, nil, pe.Wrap(ErrIllegalPacket, "")
		}

		// 3.3  Read ( <argument data> CR LF )
		count, value, err := in.ReadBytes(argBytesCount + 2)
		if count == 0 && err == nil {
			in.SetReaderIndex(backupReaderIndex)
			return false, nil, nil
		} else if err != nil {
			return false, nil, err
		}

		data[i] = value[:count-2]
	}

	return true, Command(data), nil
}

func readStringInt(in *goetty.ByteBuf) (int, int, error) {
	count, line, err := readLine(in)
	if count == 0 && err == nil {
		return 0, 0, nil
	} else if err != nil {
		return 0, 0, err
	}

	// count-2:xclude 'CR CF'
	value, err := strconv.Atoi(util.SliceToString(line[:count-2]))
	if err != nil {
		return 0, 0, err
	}

	return len(line), value, nil
}

func readLine(in *goetty.ByteBuf) (int, []byte, error) {
	offset := 0
	size := in.Readable()

	for offset < size {
		ch, _ := in.PeekByte(offset)
		if ch == gredis.LF {
			ch, _ := in.PeekByte(offset - 1)
			if ch == gredis.CR {
				return in.ReadBytes(offset + 1)
			}

			return 0, nil, ErrIllegalPacket
		}
		offset++
	}

	return 0, nil, nil
}
