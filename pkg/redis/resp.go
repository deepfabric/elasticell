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
	"fmt"
	"strconv"

	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

const (
	pong = "PONG"
)

var (
	ErrNotSupportCommand = &raftcmdpb.Response{
		ErrorResult: []byte("command is not support"),
	}

	ErrInvalidCommandResp = &raftcmdpb.Response{
		ErrorResult: []byte("invalid command"),
	}

	PongResp = &raftcmdpb.Response{
		StatusResult: []byte("PONG"),
	}

	OKStatusResp = &raftcmdpb.Response{
		StatusResult: []byte("OK"),
	}
)

func int64ToBytes(v int64) []byte {
	return strconv.AppendInt(nil, v, 10)
}

func WriteError(err []byte, buf *goetty.ByteBuf) {
	buf.WriteByte(' ')
	if err != nil {
		buf.WriteByte(' ')
		buf.Write(err)
	}
	buf.Write(delims)
}

func WriteStatus(status []byte, buf *goetty.ByteBuf) {
	buf.WriteByte('+')
	buf.Write(status)
	buf.Write(delims)
}

func WriteInteger(n int64, buf *goetty.ByteBuf) {
	buf.WriteByte(':')
	buf.Write(int64ToBytes(n))
	buf.Write(delims)
}

func WriteBulk(b []byte, buf *goetty.ByteBuf) {
	buf.WriteByte('$')
	if b == nil {
		buf.Write(nullBulk)
	} else {
		buf.Write(util.StringToSlice(strconv.Itoa(len(b))))
		buf.Write(delims)
		buf.Write(b)
	}

	buf.Write(delims)
}

func WriteArray(lst []interface{}, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if lst == nil {
		buf.Write(nullArray)
		buf.Write(delims)
	} else {
		buf.Write(util.StringToSlice(strconv.Itoa(len(lst))))
		buf.Write(delims)

		for i := 0; i < len(lst); i++ {
			switch v := lst[i].(type) {
			case []interface{}:
				WriteArray(v, buf)
			case [][]byte:
				WriteSliceArray(v, buf)
			case []byte:
				WriteBulk(v, buf)
			case nil:
				WriteBulk(nil, buf)
			case int64:
				WriteInteger(v, buf)
			case string:
				WriteStatus(util.StringToSlice(v), buf)
			case error:
				WriteError(util.StringToSlice(v.Error()), buf)
			default:
				panic(fmt.Sprintf("invalid array type %T %v", lst[i], v))
			}
		}
	}
}

func WriteSliceArray(lst [][]byte, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if lst == nil {
		buf.Write(nullArray)
		buf.Write(delims)
	} else {
		buf.Write(util.StringToSlice(strconv.Itoa(len(lst))))
		buf.Write(delims)

		for i := 0; i < len(lst); i++ {
			WriteBulk(lst[i], buf)
		}
	}
}

func WriteFVPairArray(lst []*raftcmdpb.FVPair, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if lst == nil {
		buf.Write(nullArray)
		buf.Write(delims)
	} else {
		buf.Write(util.StringToSlice(strconv.Itoa(len(lst) * 2)))
		buf.Write(delims)

		for i := 0; i < len(lst); i++ {
			WriteBulk(lst[i].Field, buf)
			WriteBulk(lst[i].Value, buf)
		}
	}
}

func WriteScorePairArray(lst []*raftcmdpb.ScorePair, withScores bool, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if lst == nil {
		buf.Write(nullArray)
		buf.Write(delims)
	} else {
		if withScores {
			buf.Write(util.StringToSlice(strconv.Itoa(len(lst) * 2)))
			buf.Write(delims)
		} else {
			buf.Write(util.StringToSlice(strconv.Itoa(len(lst))))
			buf.Write(delims)

		}

		for i := 0; i < len(lst); i++ {
			WriteBulk(lst[i].Member, buf)

			if withScores {
				WriteBulk(int64ToBytes(lst[i].Score), buf)
			}
		}
	}
}
