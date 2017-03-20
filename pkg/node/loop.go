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

package node

import (
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/log"
)

type loop struct {
	sync.RWMutex

	name string

	interval time.Duration
	ticker   *time.Ticker
	stopped  bool

	doLoop func(uint64)
	id     uint64
}

func newLoop(name string, interval time.Duration, doLoop func(uint64), id uint64) *loop {
	return &loop{
		name:   name,
		doLoop: doLoop,
		ticker: time.NewTicker(interval),
		id:     id,
	}
}

func (l *loop) start() {
	log.Infof("heartbeat: heartbeat is start, name=<%s>",
		l.name)

	// last := time.Now()

	for range l.ticker.C {
		// now := time.Now()
		// if now.After(last.Add(l.interval * 2)) {
		// 	log.Warnf("heartbeat: too late for heartbeat. name<%s>,last=<%s>, now=<%s>",
		// 		l.name,
		// 		last,
		// 		now)
		// }

		if l.isStopped() {
			log.Warn("stop: store heartbeat is stopped, exit the loop")
			return
		}

		l.doLoop(l.id)

		// last = time.Now()
	}
}

func (l *loop) stop() {
	l.ticker.Stop()
	l.setStopped()

	log.Infof("heartbeat: heartbeat is stopped, name=<%s>",
		l.name)
}

func (l *loop) setStopped() {
	l.Lock()
	defer l.Unlock()

	l.stopped = true
}

func (l *loop) isStopped() bool {
	l.RLock()
	v := l.stopped
	l.RUnlock()

	return v
}
