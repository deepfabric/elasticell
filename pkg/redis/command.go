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
