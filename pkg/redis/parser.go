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

	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	pe "github.com/pkg/errors"
)

var (
	// ErrIllegalPacket parse err
	ErrIllegalPacket = errors.New("illegal packet data")
)

const (
	// CR \r
	CR = '\r'
	// LF \n
	LF = '\n'
	// ARGBegin '$'
	ARGBegin = '$'
	// CMDBegin '*'
	CMDBegin = '*'

	defaultBufferSize = 64
)

func readCommand(in *goetty.ByteBuf) (bool, *Command, error) {
	for {
		// remember the begin read index,
		// if we found has no enough data, we will resume this read index,
		// and waiting for next.
		backupReaderIndex := in.GetReaderIndex()

		c, err := in.ReadByte()
		if err != nil {
			return false, nil, err
		}

		// 1. Read ( *<number of arguments> CR LF )
		if c != CMDBegin {
			return false, nil, pe.Wrap(ErrIllegalPacket, "1 read")
		}

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

			if c != ARGBegin {
				return false, nil, pe.Wrapf(ErrIllegalPacket, "3.2 read")
			}

			count, argBytesCount, err := readStringInt(in)
			if count == 0 && err == nil {
				in.SetReaderIndex(backupReaderIndex)
				return false, nil, nil
			} else if err != nil {
				return false, nil, err
			} else if count < 2 {
				return false, nil, pe.Wrap(ErrIllegalPacket, "3.2-a read")
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

		return true, NewCommand(data), nil
	}
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
		if ch == LF {
			ch, _ := in.PeekByte(offset - 1)
			if ch == CR {
				return in.ReadBytes(offset + 1)
			}

			return 0, nil, ErrIllegalPacket
		}
		offset++
	}

	return 0, nil, nil
}
