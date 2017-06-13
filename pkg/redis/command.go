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
	"bytes"
	"strings"

	"github.com/deepfabric/elasticell/pkg/util"
)

const (
	Set      = "set"
	Get      = "get"
	MGet     = "mget"
	MSet     = "mset"
	Keys     = "keys"
	IncrBy   = "incrby"
	DecrBy   = "decrby"
	GetSet   = "getset"
	Append   = "append"
	SetNX    = "setnx"
	StrLen   = "strlen"
	Del      = "del"
	Decr     = "decr"
	Incr     = "incr"
	SetRange = "setrange"
	MSetNX   = "msetnx"
)

// Command redis command
type Command [][]byte

// Cmd returns redis command
func (c Command) Cmd() []byte {
	return c[0]
}

// CmdString returns redis command use lower string
func (c Command) CmdString() string {
	return strings.ToLower(util.SliceToString(c[0]))
}

// Args returns redis command args
func (c Command) Args() [][]byte {
	return c[1:]
}

// ToString returns a redis command as string
func (c Command) ToString() string {
	buf := new(bytes.Buffer)
	for _, arg := range c {
		buf.Write(arg)
		buf.WriteString(" ")
	}

	return strings.TrimSpace(buf.String())
}
