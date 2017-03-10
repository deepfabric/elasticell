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
	"github.com/deepfabric/elasticell/pkg/util"
)

const (
	Del      = "del"
	Set      = "set"
	SetNX    = "setnx"
	MSet     = "mset"
	MSetNX   = "msetnx"
	Append   = "append"
	StrLen   = "strlen"
	SetRange = "setrange"
	Decr     = "decr"
	DecrBy   = "decrby"
	Incr     = "incr"
	IncrBy   = "incrby"
	Get      = "get"
	GetSet   = "getset"
	MGet     = "mget"
)

// Command redis command
type Command struct {
	Cmd  string
	Args [][]byte
}

// NewCommand create a new command
func NewCommand(data [][]byte) *Command {
	return &Command{
		Cmd:  util.SliceToString(data[0]),
		Args: data[1:],
	}
}
