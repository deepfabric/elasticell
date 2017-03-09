package server

import (
	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
)

var (
	encoder = newRedisEncoder()
	decoder = newRedisDecoder()
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
	complete, cmd, err := redis.ReadCommand(in)
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
	// reply, _ := data.(*goetty.ByteBuf)
	// out.Write(reply.R)
	return nil
}
