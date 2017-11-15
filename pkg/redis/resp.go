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
	"strconv"

	"github.com/deepfabric/elasticell/pkg/pb/querypb"
	"github.com/deepfabric/elasticell/pkg/pb/raftcmdpb"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
	gredis "github.com/fagongzi/goetty/protocol/redis"
)

const (
	pong = "PONG"
)

var (
	ErrNotSupportCommand  = []byte("command is not support")
	ErrInvalidCommandResp = []byte("invalid command")
	PongResp              = []byte("PONG")
	OKStatusResp          = []byte("OK")
)

// WriteFVPairArray write field value pair array resp
func WriteFVPairArray(lst []*raftcmdpb.FVPair, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(gredis.NullArray)
		buf.Write(gredis.Delims)
	} else {
		buf.Write(util.StringToSlice(strconv.Itoa(len(lst) * 2)))
		buf.Write(gredis.Delims)

		for i := 0; i < len(lst); i++ {
			gredis.WriteBulk(lst[i].Field, buf)
			gredis.WriteBulk(lst[i].Value, buf)
		}
	}
}

// WriteScorePairArray write score member pair array resp
func WriteScorePairArray(lst []*raftcmdpb.ScorePair, withScores bool, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(gredis.NullArray)
		buf.Write(gredis.Delims)
	} else {
		if withScores {
			buf.Write(util.StringToSlice(strconv.Itoa(len(lst) * 2)))
			buf.Write(gredis.Delims)
		} else {
			buf.Write(util.StringToSlice(strconv.Itoa(len(lst))))
			buf.Write(gredis.Delims)
		}

		for i := 0; i < len(lst); i++ {
			gredis.WriteBulk(lst[i].Member, buf)

			if withScores {
				gredis.WriteBulk(util.FormatFloat64ToBytes(lst[i].Score), buf)
			}
		}
	}
}

// WriteDocArray write doc array resp
func WriteDocArray(lst []*querypb.Document, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(gredis.NullArray)
		buf.Write(gredis.Delims)
	} else {
		buf.Write(util.StringToSlice(strconv.Itoa(len(lst))))
		buf.Write(gredis.Delims)

		for i := 0; i < len(lst); i++ {
			buf.WriteByte('*')
			buf.Write(util.StringToSlice("3"))
			buf.Write(gredis.Delims)

			//order
			orders := lst[i].Order
			buf.WriteByte('*')
			if len(orders) == 0 {
				buf.Write(gredis.NullArray)
				buf.Write(gredis.Delims)
			} else {
				buf.Write(util.StringToSlice(strconv.Itoa(len(orders))))
				buf.Write(gredis.Delims)
				for j := 0; j < len(orders); j++ {
					gredis.WriteInteger(int64(orders[j]), buf)
				}
			}
			//key
			gredis.WriteBulk(lst[i].Key, buf)
			//fvPairs
			gredis.WriteSliceArray(lst[i].FvPairs, buf)
		}
	}
}
