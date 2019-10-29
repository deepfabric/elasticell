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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/fagongzi/log"
	"github.com/deepfabric/elasticell/pkg/proxy"
	"github.com/deepfabric/elasticell/pkg/util"
)

var (
	version = flag.Bool("version", false, "Show version info")
)

func main() {
	flag.Parse()

	if *version && util.PrintVersion() {
		os.Exit(0)
	}

	log.InitLog()

	cfg := proxy.GetCfg()

	p := proxy.NewRedisProxy(cfg)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go p.Start()

	sig := <-sc

	p.Stop()
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
