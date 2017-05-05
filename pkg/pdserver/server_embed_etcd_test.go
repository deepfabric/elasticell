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

package pdserver

import (
	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

func (s *testServerSuite) TestEmbedEtcd(c *C) {
	s.restartMultiPDServer(c, 3)
	cfg, _ := s.servers[0].cfg.getEmbedEtcdConfig()
	endpoints := []string{cfg.LCUrls[0].String()}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: DefaultTimeout,
	})

	c.Assert(err, IsNil)

	key := "/hello"
	value := "hello"

	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	_, err = client.Put(ctx, key, value)
	cancel()
	c.Assert(err, IsNil)

	ctx, cancel = context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	rsp, err := clientv3.NewKV(client).Get(ctx, key)
	cancel()
	c.Assert(err, IsNil)
	c.Assert(len(rsp.Kvs), Equals, 1)
	c.Assert(string(rsp.Kvs[0].Value), Equals, value)
}
