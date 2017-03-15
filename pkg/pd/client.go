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

package pd

import (
	"time"

	"golang.org/x/net/context"

	"sync"

	"github.com/deepfabric/elasticell/pkg/log"
	pb "github.com/deepfabric/elasticell/pkg/pdpb"
	"github.com/deepfabric/elasticell/pkg/storage"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	defaultConnectTimeout = 5 * time.Second
)

// Client pd client
type Client struct {
	name string

	mut   sync.RWMutex
	addrs []string

	continuousFailureCount int64
	conn                   *grpc.ClientConn
	pd                     pb.PDServiceClient
	lastAddr               string

	seq uint64
}

// NewClient create a pd client use init pd pdAddrs
func NewClient(name string, initAddrs ...string) (*Client, error) {
	c := new(Client)

	log.Debugf("pd-client: initial pds, pds=<%v>",
		initAddrs)

	c.name = name
	c.addrs = append(c.addrs, initAddrs...)
	c.seq = 0

	err := c.resetConn()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Close close conn
func (c *Client) Close() error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if nil != c.conn {
		return c.conn.Close()
	}

	return nil
}

//GetLastPD returns last pd server
func (c *Client) GetLastPD() string {
	c.mut.RLock()
	defer c.mut.RUnlock()
	return c.lastAddr
}

func (c *Client) resetConn() error {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	if c.continuousFailureCount > int64(len(c.addrs)) {
		time.Sleep(time.Second * 10)
	}

	var conn *grpc.ClientConn
	var err error
	for _, addr := range c.addrs {
		log.Debugf("pd-client: try to connect to pd, target=<%s>",
			addr)

		conn, err = createConn(addr)
		if err != nil {
			log.Warnf("pd-client: connect to pd server failure, addr=<%s>, errors: %v",
				addr,
				err)
			c.continuousFailureCount++
			continue
		} else {
			c.lastAddr = addr
			c.continuousFailureCount = 0
			break
		}
	}

	if nil == conn {
		return errors.New("pd-client connect to all init pd servers failure")
	}

	c.conn = conn
	c.pd = pb.NewPDServiceClient(conn)

	log.Infof("pd-client: connect to pd server succ, addr=<%s>", c.lastAddr)

	return nil
}

func createConn(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr,
		grpc.WithInsecure(),
		grpc.WithTimeout(defaultConnectTimeout),
		grpc.WithBlock())
}

// GetLeader get leader
func (c *Client) GetLeader(ctx context.Context, req *pb.LeaderReq) (string, error) {
	c.mut.RLock()

	if req.From == "" {
		req.From = c.name
	}

	if req.Id == 0 {
		req.Id = c.seq
		c.seq++
	}

	rsp, err := c.pd.GetLeader(ctx, req, grpc.FailFast(true))
	if err != nil {
		c.mut.RUnlock()
		if needRetry(err) {
			err = c.resetConn()
			if err != nil {
				return "", err
			}

			return c.GetLeader(ctx, req)
		}

		return "", err
	}

	c.mut.RUnlock()
	return rsp.GetLeader().Addr, nil
}

// AllocID ask pd for a uniq id
func (c *Client) AllocID() (int64, error) {
	return 0, nil
}

// GetClusterID get cluster id from pd
func (c *Client) GetClusterID() (int64, error) {
	return 0, nil
}

// IsClusterBootstrapped ask pd, the cluster is bootstrapped.
func (c *Client) IsClusterBootstrapped(ctx context.Context, req *pb.IsClusterBootstrapReq) (*pb.IsClusterBootstrapRsp, error) {
	c.mut.RLock()

	if req.From == "" {
		req.From = c.name
	}

	if req.Id == 0 {
		req.Id = c.seq
		c.seq++
	}

	rsp, err := c.pd.IsClusterBootstrap(ctx, req, grpc.FailFast(true))
	if err != nil {
		c.mut.RUnlock()
		if needRetry(err) {
			err = c.resetConn()
			if err != nil {
				return nil, err
			}

			return c.IsClusterBootstrapped(ctx, req)
		}

		return nil, err
	}

	c.mut.RUnlock()
	return rsp, nil
}

// BootstrapCluster tell pd to bootstart cluster.
func (c *Client) BootstrapCluster(ctx context.Context, req *pb.BootstrapClusterReq) (*pb.BootstrapClusterRsp, error) {
	return nil, nil
}

// TellPDStoreStarted tell pd the store on this node is started.
func (c *Client) TellPDStoreStarted(store *storage.Store) error {
	return nil
}

func needRetry(err error) bool {
	code := grpc.Code(err)

	return codes.Unavailable == code ||
		codes.FailedPrecondition == code
}
