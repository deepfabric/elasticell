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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/server"
	"github.com/deepfabric/etcd/raft"
)

var (
	pprof = flag.String("pprof-addr", "", "pprof http server address")
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

	cfg := server.GetCfg()
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
