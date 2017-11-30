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
	"errors"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/codec"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/fagongzi/goetty"
	"golang.org/x/net/context"
)

var (
	// ErrWatcherStopped watcher is stopped
	ErrWatcherStopped = errors.New("watcher is stopped")
)

// Watcher is watch pd event
type Watcher struct {
	sync.RWMutex
	addr      string
	eventFlag uint32
	client    *Client
	heartbeat time.Duration
	listen    *goetty.Server
	readyC    chan *pdpb.WatchEvent
	ctx       context.Context
	cancel    context.CancelFunc
	// about sync protocol
	localOffset, serverOffset uint64
	syncing                   bool
}

// NewWatcher returns a watcher.
// The watcher will listen form pd at the addr parameter
func NewWatcher(client *Client, addr string, eventFlag uint32, heartbeat time.Duration) *Watcher {
	return &Watcher{
		client:    client,
		addr:      addr,
		eventFlag: eventFlag,
		heartbeat: heartbeat,
	}
}

// Ready returns the event
// return a error if watcher was stopped
func (w *Watcher) Ready() (*pdpb.WatchEvent, error) {
	r, ok := <-w.readyC
	if !ok {
		return nil, ErrWatcherStopped
	}

	return r, nil
}

// Start start the watch
// If watcher was started, use Ready method in a loop to receive the newest notify
func (w *Watcher) Start() error {
	w.reset()

	errCh := make(chan error)
	go func() {
		errCh <- w.listen.Start(w.doConnection)
	}()

	select {
	case err := <-errCh:
		return err
	case <-w.listen.Started():
		_, err := w.client.RegisterWatcher(context.TODO(), &pdpb.RegisterWatcherReq{
			Watcher: pdpb.Watcher{
				Addr:      w.addr,
				EventFlag: w.eventFlag,
			},
		})
		if err != nil {
			return err
		}

		w.startHeartbeat()
		return nil
	}
}

// Stop stop the watcher
func (w *Watcher) Stop() {
	w.Lock()
	defer w.Unlock()

	if w.cancel != nil {
		w.cancel()
	}

	if w.listen != nil {
		w.listen.Stop()
	}

	if w.readyC != nil {
		close(w.readyC)
	}
}

func (w *Watcher) reset() {
	w.Lock()
	w.ctx, w.cancel = context.WithCancel(context.TODO())
	w.localOffset = 0
	w.serverOffset = 0
	w.readyC = make(chan *pdpb.WatchEvent, 32)
	w.listen = goetty.NewServer(w.addr,
		&codec.ProxyDecoder{},
		&codec.ProxyEncoder{},
		goetty.NewInt64IDGenerator())
	w.Unlock()
}

func (w *Watcher) startHeartbeat() {
	go func() {
		ticker := time.NewTicker(w.heartbeat)
		defer ticker.Stop()

		for {
			select {
			case <-w.ctx.Done():
				log.Infof("stop: watcher heartbeat stopped")
				return
			case <-ticker.C:
				w.doHeartbeat()
			}
		}
	}()
}

func (w *Watcher) doHeartbeat() {
	rsp, err := w.client.WatcherHeartbeat(context.TODO(), &pdpb.WatcherHeartbeatReq{
		Addr:   w.addr,
		Offset: w.getLocalOffset(),
	})

	if err != nil {
		log.Infof("watcher: watcher heartbeat failed: errors:\n%+v",
			err)
		return
	}

	if rsp.Paused {
		// If we are paused from pd, we need refresh all ranges
		w.resetLocalOffset(0)
		w.initNotify()
	}
}

func (w *Watcher) getServerOffset() uint64 {
	w.RLock()
	v := w.serverOffset
	w.RUnlock()
	return v
}

func (w *Watcher) resetServerOffset(offset uint64) {
	w.Lock()
	w.serverOffset = offset
	w.Unlock()
}

func (w *Watcher) resetLocalOffset(offset uint64) {
	w.Lock()
	w.localOffset = offset
	w.Unlock()
}

func (w *Watcher) getLocalOffset() uint64 {
	w.RLock()
	v := w.localOffset
	w.RUnlock()
	return v
}

func (w *Watcher) initNotify() {
	w.Lock()
	defer w.Unlock()

	w.doNotify(&pdpb.WatchEvent{
		Event: EventInit,
	})
}

func (w *Watcher) syncNotify(events ...*pdpb.WatchEvent) {
	w.Lock()
	defer w.Unlock()

	w.doNotify(events...)
}

func (w *Watcher) doNotify(events ...*pdpb.WatchEvent) {
	if w.readyC == nil {
		return
	}

	for _, e := range events {
		if e.Event > 0 {
			w.readyC <- e
		}
	}
}

func (w *Watcher) doConnection(conn goetty.IOSession) error {
	addr := conn.RemoteAddr()
	w.resetLocalOffset(0)

	for {
		msg, err := conn.Read()
		if err != nil {
			log.Errorf("notify-[%s]: read notify failed, errors\n %+v",
				addr,
				err)
			return err
		}

		if nt, ok := msg.(*pdpb.WatcherNotify); ok {
			if nt.Offset == 0 || nt.Offset > w.getLocalOffset() {
				w.resetServerOffset(nt.Offset)
				err := w.sync(conn)
				if err != nil {
					log.Errorf("notify-[%s]: sync notify failed, errors\n %+v",
						addr,
						err)
					return err
				}
			}
		} else if rsp, ok := msg.(*pdpb.WatcherNotifyRsp); ok {
			w.syncing = false
			w.resetLocalOffset(rsp.Offset)
			w.syncNotify(rsp.Events...)
			if w.getLocalOffset() < w.getServerOffset() {
				err := w.sync(conn)
				if err != nil {
					log.Errorf("notify-[%s]: sync notify failed, errors\n %+v",
						addr,
						err)
					return err
				}
			}
		}
	}
}

func (w *Watcher) sync(conn goetty.IOSession) error {
	if !w.syncing {
		err := conn.Write(&pdpb.WatcherNotifySync{
			Offset: w.getLocalOffset(),
		})
		if err != nil {
			return err
		}
		w.syncing = true
	}

	return nil
}
