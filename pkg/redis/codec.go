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
	"github.com/fagongzi/goetty"
)

var (
	// Encoder redis protocol encoder
	Encoder = newRedisEncoder()
	// Decoder redis protocol decoder
	Decoder = newRedisDecoder()
)

type redisDecoder struct {
}

type redisEncoder struct {
}

func newRedisDecoder() *redisDecoder {
	return &redisDecoder{}
}

func newRedisEncoder() *redisEncoder {
	return &redisEncoder{}
}

// Decode decode
func (decoder redisDecoder) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	complete, cmd, err := readCommand(in)
	if err != nil {
		return true, nil, err
	}

	if !complete {
		return false, nil, nil
	}

	return true, cmd, nil
}

// Encode encode
func (e redisEncoder) Encode(data interface{}, out *goetty.ByteBuf) error {
	return nil
}
