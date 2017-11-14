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
	"os"
	"os/signal"
	"syscall"

	"github.com/deepfabric/elasticell/pkg/log"
	server "github.com/deepfabric/elasticell/pkg/pdserver"
	"github.com/deepfabric/elasticell/pkg/util"
)

func main() {
	flag.Parse()

	log.InitLog()
	cfg := server.GetCfg()

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
