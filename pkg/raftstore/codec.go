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
	"github.com/deepfabric/elasticell/pkg/pb/mraft"
	"github.com/fagongzi/goetty"
)

var (
	decoder = goetty.NewIntLengthFieldBasedDecoder(newRaftDecoder())
	encoder = newRaftEncoder()
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
	_, data, err := in.ReadMarkedBytes()
	if err != nil {
		return true, nil, err
	}

	msg := &mraft.RaftMessage{}
	err = msg.Unmarshal(data)
	if err != nil {
		return true, nil, err
	}

	return true, msg, nil
}

func (e raftEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	msg := data.(*mraft.RaftMessage)
	d, err := msg.Marshal()
	if err != nil {
		return err
	}

	out.WriteInt(len(d))
	out.Write(d)
	return nil
}
