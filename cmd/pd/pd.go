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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	server "github.com/deepfabric/elasticell/pkg/pdserver"
	"github.com/deepfabric/elasticell/pkg/util"
)

var (
	name                        = flag.String("name", "", "PD instance name")
	dataPath                    = flag.String("data", "", "The data dir")
	addrRPC                     = flag.String("addr-rpc", "", "Addr: RPC")
	labelsLocation              = flag.String("labels-location", "zone,rack", "Labels: Store location label name")
	limitReplicas               = flag.Int("limit-replicas", 3, "Limit: Count of peer replicas")
	limitSnapshots              = flag.Uint64("limit-snapshots", 3, "Limit: If the snapshot count of one store is greater than this value,it will never be used as a source or target store")
	limitStoreDownSec           = flag.Int("interval-store-down", 3600, "Interval(sec): Max store down")
	limitScheduleLeader         = flag.Uint64("limit-schedule-leader", 16, "Limit: Max count of transfer leader operator")
	limitScheduleCell           = flag.Uint64("limit-schedule-cell", 12, "Limit: Max count of move cell operator")
	limitScheduleReplica        = flag.Uint64("limit-schedule-replica", 16, "Limit: Max count of add replica operator")
	thresholdStorageRate        = flag.Int("threshold-rate-storage", 80, "Threshold: Max storage rate of used for schduler")
	thresholdPauseWatcher       = flag.Int("threshold-pause-watcher", 10, "Threshold: Pause watcher, after N heartbeat times")
	intervalLeaderLeaseSec      = flag.Int64("interval-leader-lease", 5, "Interval(sec): PD leader lease")
	intervalHeartbeatWatcherSec = flag.Int("interval-heartbeat-watcher", 5, "Interval(sec): Watcher heartbeat")
	urlsClient                  = flag.String("urls-client", "http://127.0.0.1:2371", "URLS: embed etcd client urls")
	urlsAdvertiseClient         = flag.String("urls-advertise-client", "", "URLS(advertise): embed etcd client urls")
	urlsPeer                    = flag.String("urls-peer", "http://127.0.0.1:2381", "URLS: embed etcd peer urls")
	urlsAdvertisePeer           = flag.String("urls-advertise-peer", "", "URLS(advertise): embed etcd peer urls")
	initialCluster              = flag.String("initial-cluster", "", "Initial: embed etcd initial cluster")
	initialClusterState         = flag.String("initial-cluster-state", "new", "Initial: embed etcd initial cluster state")
)

func main() {
	flag.Parse()
	cfg := parseCfg()

	log.InitLog()

	var logFile, etcdLogFile string
	logFile = log.GetLogFile()
	etcdLogFile = util.ReplaceFpExt(logFile, "-etcd.log")

	f, err := os.OpenFile(etcdLogFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	server.RedirectEmbedEtcdLog(f)

	s := server.NewServer(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go s.Start()

	sig := <-sc
	s.Stop()
	log.Infof("exit: signal=<%d>.", sig)
	switch sig {
	case syscall.SIGTERM:
		log.Infof("exit: bye :-).")
		os.Exit(0)
	default:
		log.Infof("exit: bye :-(.")
		os.Exit(1)
	}
}

func parseCfg() *server.Cfg {
	if *name == "" {
		fmt.Println("PD name must be set")
		os.Exit(-1)
	}

	if *dataPath == "" {
		fmt.Println("PD data path must be set")
		os.Exit(-1)
	}

	if *addrRPC == "" {
		fmt.Println("PD rpc addr must be set")
		os.Exit(-1)
	}

	if *urlsPeer == "" {
		fmt.Println("PD embed etcd peer urls must be set")
		os.Exit(-1)
	}

	if *initialCluster == "" {
		fmt.Println("PD embed etcd embed etcd initial cluster must be set")
		os.Exit(-1)
	}

	cfg := &server.Cfg{}
	cfg.Name = *name
	cfg.DataPath = *dataPath
	cfg.AddrRPC = *addrRPC
	cfg.DurationLeaderLease = *intervalLeaderLeaseSec
	cfg.DurationHeartbeatWatcher = time.Second * time.Duration(*intervalHeartbeatWatcherSec)
	cfg.ThresholdPauseWatcher = *thresholdPauseWatcher
	cfg.URLsClient = *urlsClient
	cfg.URLsAdvertiseClient = *urlsAdvertiseClient
	cfg.URLsPeer = *urlsPeer
	cfg.URLsAdvertisePeer = *urlsAdvertisePeer
	cfg.InitialCluster = *initialCluster
	cfg.InitialClusterState = *initialClusterState
	cfg.LabelsLocation = strings.Split(*labelsLocation, ",")
	cfg.LimitReplicas = uint32(*limitReplicas)
	cfg.LimitSnapshots = *limitSnapshots
	cfg.LimitStoreDownDuration = time.Second * time.Duration(*limitStoreDownSec)
	cfg.LimitScheduleLeader = *limitScheduleLeader
	cfg.LimitScheduleCell = *limitScheduleCell
	cfg.LimitScheduleReplica = *limitScheduleReplica
	cfg.ThresholdStorageRate = *thresholdStorageRate

	return cfg
}
