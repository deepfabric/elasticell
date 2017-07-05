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
	"fmt"

	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

var (
	decoder = goetty.NewIntLengthFieldBasedDecoder(newRaftDecoder())
	encoder = newRaftEncoder()
)

const (
	typeRaft = 1
	typeSnap = 2
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

	_, data, err := in.ReadMarkedBytes()
	if err != nil {
		return true, nil, err
	}

	switch t {
	case typeSnap:
		msg := &mraft.SnapshotData{}
		util.MustUnmarshal(msg, data)
		return true, msg, nil
	case typeRaft:
		msg := &mraft.RaftMessage{}
		util.MustUnmarshal(msg, data)
		return true, msg, nil
	}

	return true, nil, fmt.Errorf("decoder: not support msg type, type=<%d>", t)
}

func (e raftEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	if msg, ok := data.(*mraft.RaftMessage); ok {
		d := util.MustMarshal(msg)
		out.WriteInt(len(d) + 1)
		out.WriteByte(typeRaft)
		out.Write(d)
	} else if msg, ok := data.(*mraft.SnapshotData); ok {
		d := util.MustMarshal(msg)
		out.WriteInt(len(d) + 1)
		out.WriteByte(typeSnap)
		out.Write(d)
	}

	return nil
}
