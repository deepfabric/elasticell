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

package raftstore

import (
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/pool"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

var (
	decoder = goetty.NewIntLengthFieldBasedDecoder(newRaftDecoder())
	encoder = newRaftEncoder()
)

const (
	typeRaft      = 1
	typeSnap      = 2
	typeSnapData  = 3
	typeSnapEnd   = 4
	typeHeartbeat = 5
)

type heartbeatMsg struct{}

func (h *heartbeatMsg) Size() int {
	return 0
}

func (h *heartbeatMsg) Marshal() ([]byte, error) {
	return nil, nil
}

func (h *heartbeatMsg) MarshalTo(data []byte) (int, error) {
	return 0, nil
}

var (
	heartbeat = &heartbeatMsg{}
)

type raftDecoder struct {
}

type raftEncoder struct {
}

func newRaftDecoder() *raftDecoder {
	return &raftDecoder{}
}

func newRaftEncoder() *raftEncoder {
	return &raftEncoder{}
}

func (decoder raftDecoder) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	t, err := in.ReadByte()
	if err != nil {
		return true, nil, err
	}

	data := in.GetMarkedRemindData()

	switch t {
	case typeSnap:
		msg := &mraft.SnapKey{}
		util.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeSnapData:
		msg := &mraft.SnapshotData{}
		util.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeSnapEnd:
		msg := &mraft.SnapshotDataEnd{}
		util.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeRaft:
		msg := pool.AcquireRaftMessage()
		util.MustUnmarshal(msg, data)
		in.MarkedBytesReaded()
		return true, msg, nil
	case typeHeartbeat:
		in.MarkedBytesReaded()
		// ignore the heartbeat msg
		return false, nil, nil
	}

	log.Fatalf("bug: not support msg type, type=<%d>", t)
	return false, nil, nil
}

func (e raftEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	t := typeRaft
	var m util.Marashal

	if msg, ok := data.(*mraft.RaftMessage); ok {
		t = typeRaft
		m = msg
	} else if msg, ok := data.(*mraft.SnapKey); ok {
		t = typeSnap
		m = msg
	} else if msg, ok := data.(*mraft.SnapshotData); ok {
		t = typeSnapData
		m = msg
	} else if msg, ok := data.(*mraft.SnapshotDataEnd); ok {
		t = typeSnapEnd
		m = msg
	} else if msg, ok := data.(*heartbeatMsg); ok {
		t = typeHeartbeat
		m = msg
	} else {
		log.Fatalf("bug: unsupport msg: %+v", msg)
	}

	size := m.Size()
	out.WriteInt(size + 1)
	out.WriteByte(byte(t))

	if size > 0 {
		index := out.GetWriteIndex()
		out.Expansion(size)
		util.MustMarshalTo(m, out.RawBuf()[index:index+size])
		out.SetWriterIndex(index + size)
	}

	return nil
}
