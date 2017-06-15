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
	"bytes"
	"fmt"
	"io/ioutil"
)

var (
	testPort        = 10000
	baseAddrPattern = "127.0.0.1:%d"
	httpAddrPattern = "http://127.0.0.1:%d"
	testNamePattern = "test-pd-%d"
)

func getTestPort() int {
	testPort++
	return testPort
}

func genBaseAddr() string {
	return fmt.Sprintf(baseAddrPattern, getTestPort())
}

func genHTTPAddr() string {
	return fmt.Sprintf(httpAddrPattern, getTestPort())
}

func getTestName(index int) string {
	return fmt.Sprintf(testNamePattern, index)
}

// NewTestSingleServer returns a single pd server
func NewTestSingleServer() *Server {
	name := "test-single-pd"
	addrPeer := genHTTPAddr()
	addrClient := genHTTPAddr()
	addrRPC := genBaseAddr()
	return NewServer(newTestConfig(name,
		addrClient,
		addrPeer,
		addrRPC,
		fmt.Sprintf("%s=%s", name, addrPeer)))
}

// NewTestMultiServers returns multi pd server
func NewTestMultiServers(count int) []*Server {
	var servers []*Server
	var names []string
	var addrClients []string
	var addrPeers []string
	var addrRPCs []string

	buf := bytes.NewBufferString("")

	for index := 0; index < count; index++ {
		name := getTestName(index)
		addrPeer := genHTTPAddr()
		addrClient := genHTTPAddr()
		addrRPC := genBaseAddr()

		names = append(names, name)
		addrClients = append(addrClients, addrClient)
		addrPeers = append(addrPeers, addrPeer)
		addrRPCs = append(addrRPCs, addrRPC)

		buf.WriteString(fmt.Sprintf("%s=%s", name, addrPeer))
		if index < count-1 {
			buf.WriteString(",")
		}
	}

	initCluster := string(buf.Bytes())

	for index := 0; index < count; index++ {
		cfg := newTestConfig(names[index],
			addrClients[index],
			addrPeers[index],
			addrRPCs[index],
			initCluster)

		servers = append(servers, NewServer(cfg))
	}

	return servers
}

func newTestConfig(name, addrClient, addrPeer, addrRPC, initCluster string) *Cfg {
	cfg := &Cfg{
		EmbedEtcd: &EmbedEtcdCfg{},
		Schedule:  &ScheduleCfg{},
	}

	cfg.Name = name
	cfg.DataDir, _ = ioutil.TempDir("/tmp", cfg.Name)
	cfg.LeaseSecsTTL = 1
	cfg.RPCAddr = addrRPC

	cfg.EmbedEtcd.ClientUrls = addrClient
	cfg.EmbedEtcd.PeerUrls = addrPeer
	cfg.EmbedEtcd.InitialCluster = initCluster
	cfg.EmbedEtcd.InitialClusterState = "new"

	cfg.Schedule.MaxReplicas = 3
	cfg.Schedule.LocationLabels = []string{"zone", "rack"}
	cfg.Schedule.MaxSnapshotCount = 3
	cfg.Schedule.MaxStoreDownTimeMs = 1000
	cfg.Schedule.LeaderScheduleLimit = 16
	cfg.Schedule.CellScheduleLimit = 12
	cfg.Schedule.ReplicaScheduleLimit = 16

	return cfg
}
