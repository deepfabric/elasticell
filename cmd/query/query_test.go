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

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/deepfabric/elasticell/pkg/pb/querypb"

	"github.com/deepfabric/elasticell/pkg/redis"
	"github.com/fagongzi/goetty"
	gredis "github.com/fagongzi/goetty/protocol/redis"
)

func TestQueryCodec(t *testing.T) {
	var rsp interface{}
	var ok bool
	var err error
	conn := goetty.NewConnector(&goetty.Conf{
		Addr: *addr,
		TimeoutConnectToServer: time.Second * time.Duration(*connectTimeout),
	}, gredis.NewRedisReplyDecoder(), goetty.NewEmptyEncoder())
	if _, err = conn.Connect(); err != nil {
		return
	}
	gredis.InitRedisConn(conn)
	buf := conn.OutBuf()

	docs := []*querypb.Document{
		&querypb.Document{
			Order: []uint64{1, 2},
			Key:   []byte("order_1"),
		},
		&querypb.Document{
			Order: []uint64{2, 5},
			Key:   []byte("order_3"),
		},
	}
	redis.WriteDocArray(docs, buf)

	rrd := gredis.NewRedisReplyDecoder()
	ok, rsp, err = rrd.Decode(buf)
	fmt.Printf("ok %+v, rsp %+v, err %+v\n", ok, rsp, err)
	if err != nil {
		t.Fatalf("error %+v", err)
	}
	s := rsp.([]interface{})
	for _, item := range s {
		fmt.Printf("%+v\n", item)
	}
	return
}
