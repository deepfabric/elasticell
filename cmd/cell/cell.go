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
	"bytes"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	runPprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/metapb"
	"github.com/deepfabric/elasticell/pkg/server"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/deepfabric/etcd/raft"
)

const (
	kb = 1024
	mb = 1024 * kb

	defaultCapacity = 96
)

var (
	clusterID      = flag.Uint64("clusterid", 0, "Cluster ID")
	pd             = flag.String("pd", "", "PD addresses")
	addr           = flag.String("addr", ":10800", "Internal address")
	addrCli        = flag.String("addr-cli", ":6379", "KV client address")
	pprof          = flag.String("addr-pprof", "", "pprof http server address")
	dataDir        = flag.String("data", "", "The data dir")
	zone           = flag.String("zone", "", "Zone label")
	rack           = flag.String("rack", "", "Rack label")
	bufferCliRead  = flag.Int("buffer-cli-read", 256, "Buffer(bytes): bytes of KV client read")
	bufferCliWrite = flag.Int("buffer-cli-write", 256, "Buffer(bytes): bytes of KV client write")
	batchCliResps  = flag.Int64("batch-cli-resps", 64, "Batch: Max count of responses in a write operation")

	// raftstore
	cellCapacityMB            = flag.Uint64("capacity-cell", 0, "Capacity(MB): cell")
	intervalHeartbeatStore    = flag.Int("interval-heartbeat-store", 10, "Interval(sec): Store heartbeat")
	intervalHeartbeatCell     = flag.Int("interval-heartbeat-cell", 30, "Interval(sec): Cell heartbeat")
	intervalSplitCheck        = flag.Int("interval-split-check", 10, "Interval(sec): Split check")
	intervalCompact           = flag.Int("interval-compact", 10, "Interval(sec): Compact raft log")
	intervalReportMetric      = flag.Int("interval-report-metric", 10, "Interval(sec): Report cell metric")
	intervalRaftTick          = flag.Int("interval-raft-tick", 1000, "Interval(ms): Raft tick")
	intervalRetrySentSnapshot = flag.Int("interval-retry-sent-snapshot", 10, "Interval(sec): Retry sent snapshot")
	limitPeerDown             = flag.Uint64("limit-peer-down", 5*60, "Limit(sec): Max peer downtime")
	limitCompactCount         = flag.Uint64("limit-compact-count", 0, "Limit: Count of raft logs, if reach this limit, leader will compact [first,applied], otherwise [first, minimum replicated]")
	limitCompactBytesMB       = flag.Uint64("limit-compact-bytes", 0, "Limit(MB): Total bytes of raft logs, if reach this limit, leader will compact [first,applied], otherwise [first, minimum replicated]")
	limitCompactLag           = flag.Uint64("limit-compact-lag", 0, "Limit: Max count of lag log, leader will compact [first, compact - lag], avoid send snapshot file to a little lag peer")
	limitRaftMsgCount         = flag.Int("limit-raft-msg-count", 256, "Limit: Max count of in-flight raft append messages")
	limitRaftMsgBytesMB       = flag.Uint64("limit-raft-msg-bytes", 1, "Limit(MB): Max bytes per raft msg")
	limitRaftEntryBytesMB     = flag.Uint64("limit-raft-entry-bytes", 8, "Limit(MB): Max bytes of raft log entry")
	limitSnapChunkBytesKB     = flag.Uint64("limit-snap-chunk-bytes", 1024, "Limit(KB): Max snap chunk size")
	limitSnapChunkRate        = flag.Uint64("limit-snap-chunk-rate", 16, "Limit: Max snap chunks sent per second")
	thresholdCompact          = flag.Uint64("threshold-compact", 64, "Threshold: Raft Log compact, count of [first, replicated]")
	thresholdSplitCheckMB     = flag.Uint64("threshold-split-check", 0, "Threshold(MB): Start split check, bytes that the cell has bean stored")
	thresholdRaftElection     = flag.Int("threshold-raft-election", 10, "Threshold: Raft election, after this ticks")
	thresholdRaftHeartbeat    = flag.Int("threshold-raft-heartbeat", 2, "Threshold: Raft heartbeat, after this ticks")
	batchSizeProposal         = flag.Uint64("batch-size-proposal", 1024, "Batch: Max commands in a proposal.")
	batchSizeSent             = flag.Uint64("batch-size-sent", 64, "Batch: Max size of send msgs")
	workerCountSent           = flag.Uint64("worker-count-sent", 64, "Worker count: sent internal messages")
	workerCountSentSnap       = flag.Uint64("worker-count-sent-snap", 4, "Worker count: sent snap messages")
	workerCountApply          = flag.Uint64("worker-count-apply", 64, "Worker count: apply raft log")
	enableMetricsRequest      = flag.Bool("enable-metrics-request", false, "Enable: request metrics")

	// metric
	metricJob          = flag.String("metric-job", "", "prometheus job name")
	metricAddress      = flag.String("metric-address", "", "prometheus proxy address")
	metricIntervalSync = flag.Uint64("interval-metric-sync", 0, "Interval(sec): metric sync")
)

func main() {
	flag.Parse()

	log.InitLog()
	raft.SetLogger(log.DefaultLogger())

	if "" != *pprof {
		log.Infof("bootstrap: start pprof at: %s", *pprof)
		go func() {
			log.Fatalf("bootstrap: start pprof failed, errors:\n%+v",
				http.ListenAndServe(*pprof, nil))
		}()
	}

	s := server.NewServer(parseCfg())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGUSR1)

	go s.Start()

	for {
		sig := <-sc
		switch sig {
		case syscall.SIGUSR1:
			buf := bytes.NewBuffer([]byte{})
			_ = runPprof.Lookup("goroutine").WriteTo(buf, 1)
			log.Infof("got signal=<%d>.", sig)
			log.Infof(buf.String())
			continue
		default:
			retVal := 0
			if sig != syscall.SIGTERM {
				retVal = 1
			}
			log.Infof("exit: signal=<%d>.", sig)
			s.Stop()
			log.Infof("exit: bye :-).")
			os.Exit(retVal)
		}
	}
}

func parseCfg() *server.Cfg {
	if *pd == "" {
		fmt.Println("PD must be set")
		os.Exit(-1)
	}

	if *dataDir == "" {
		fmt.Println("Data dir must be set")
		os.Exit(-1)
	}

	if *rack == "" {
		fmt.Println("Location rack must be set")
		os.Exit(-1)
	}

	if *zone == "" {
		fmt.Println("Location zone must be set")
		os.Exit(-1)
	}

	adjust()

	cfg := server.NewCfg()

	cfg.Node.ClusterID = *clusterID
	cfg.Node.PDEndpoints = strings.Split(*pd, ",")
	cfg.Node.RaftStore.Addr = *addr
	cfg.AddrCli = *addrCli
	cfg.Node.RaftStore.DataPath = *dataDir
	cfg.Node.StoreLables = append(cfg.Node.StoreLables, metapb.Label{
		Key:   "zone",
		Value: *zone,
	})
	cfg.Node.StoreLables = append(cfg.Node.StoreLables, metapb.Label{
		Key:   "rack",
		Value: *rack,
	})
	cfg.BufferCliRead = *bufferCliRead
	cfg.BufferCliWrite = *bufferCliWrite
	cfg.BatchCliResps = *batchCliResps

	cfg.Node.RaftStore.CellCapacity = *cellCapacityMB * mb
	cfg.Node.RaftStore.DurationHeartbeatStore = time.Second * time.Duration(*intervalHeartbeatStore)
	cfg.Node.RaftStore.DurationHeartbeatCell = time.Second * time.Duration(*intervalHeartbeatCell)
	cfg.Node.RaftStore.DurationSplitCheck = time.Second * time.Duration(*intervalSplitCheck)
	cfg.Node.RaftStore.DurationCompact = time.Second * time.Duration(*intervalCompact)
	cfg.Node.RaftStore.DurationReportMetric = time.Second * time.Duration(*intervalReportMetric)
	cfg.Node.RaftStore.DurationRaftTick = time.Millisecond * time.Duration(*intervalRaftTick)
	cfg.Node.RaftStore.DurationRetrySentSnapshot = time.Second * time.Duration(*intervalRetrySentSnapshot)
	cfg.Node.RaftStore.LimitPeerDownDuration = time.Second * time.Duration(*limitPeerDown)
	cfg.Node.RaftStore.LimitCompactCount = *limitCompactCount
	cfg.Node.RaftStore.LimitCompactBytes = *limitCompactBytesMB * mb
	cfg.Node.RaftStore.LimitCompactLag = *limitCompactLag
	cfg.Node.RaftStore.LimitRaftMsgCount = *limitRaftMsgCount
	cfg.Node.RaftStore.LimitRaftMsgBytes = *limitRaftMsgBytesMB * mb
	cfg.Node.RaftStore.LimitRaftEntryBytes = *limitRaftEntryBytesMB * mb
	cfg.Node.RaftStore.LimitSnapChunkBytes = *limitSnapChunkBytesKB * kb
	cfg.Node.RaftStore.LimitSnapChunkRate = *limitSnapChunkRate
	cfg.Node.RaftStore.ThresholdCompact = *thresholdCompact
	cfg.Node.RaftStore.ThresholdSplitCheckBytes = *thresholdSplitCheckMB * mb
	cfg.Node.RaftStore.ThresholdRaftElection = *thresholdRaftElection
	cfg.Node.RaftStore.ThresholdRaftHeartbeat = *thresholdRaftHeartbeat
	cfg.Node.RaftStore.BatchSizeProposal = *batchSizeProposal
	cfg.Node.RaftStore.BatchSizeSent = *batchSizeSent
	cfg.Node.RaftStore.WorkerCountSent = *workerCountSent
	cfg.Node.RaftStore.WorkerCountSentSnap = *workerCountSentSnap
	cfg.Node.RaftStore.WorkerCountApply = *workerCountApply
	cfg.Node.RaftStore.EnableMetricsRequest = *enableMetricsRequest

	cfg.Metric = util.NewMetricCfg(*metricJob, *metricAddress, time.Second*time.Duration(*metricIntervalSync))
	return cfg
}

func adjust() {
	if *cellCapacityMB == 0 {
		*cellCapacityMB = defaultCapacity
	}

	if *limitCompactCount == 0 {
		*limitCompactCount = *cellCapacityMB * mb * 3 / 4 / kb
	}

	if *limitCompactBytesMB == 0 {
		*limitCompactBytesMB = *cellCapacityMB * 3 / 4
	}

	if *limitCompactLag == 0 {
		*limitCompactLag = *cellCapacityMB * mb / 256 / 16
	}

	if *thresholdSplitCheckMB == 0 {
		*thresholdSplitCheckMB = *cellCapacityMB / 16
	}
}
