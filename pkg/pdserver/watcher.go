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
	"errors"
	"sync"
	"time"

	"github.com/deepfabric/elasticell/pkg/codec"
	"github.com/deepfabric/elasticell/pkg/log"
	"github.com/deepfabric/elasticell/pkg/pb/pdpb"
	"github.com/deepfabric/elasticell/pkg/pd"
	"github.com/deepfabric/elasticell/pkg/util"
	"github.com/fagongzi/goetty"
)

var (
	errConnect = errors.New("not connected")
)

const (
	ready = iota
	paused
)

func cmp(a, b interface{}) bool {
	return a.(*notify).watcher == b.(*notify).watcher
}

type notify struct {
	watcher string
	offset  uint64
}

func newWatcherState(watcher pdpb.Watcher) *watcherState {
	return &watcherState{
		watcher: watcher,
		state:   ready,
		q:       util.NewOffsetQueue(),
	}
}

type watcherState struct {
	watcher          pdpb.Watcher
	state            int
	hbTimeout        *goetty.Timeout
	q                *util.OffsetQueue
	lastNotifyOffset uint64
}

func (state *watcherState) reset() {
	state.cancelTimeout()
	state.state = ready
	state.q = util.NewOffsetQueue()
	state.lastNotifyOffset = 0
}

func (state *watcherState) pause() {
	state.reset()
	state.state = paused
	log.Warnf("notify: %s reset to pause", state.watcher.String())
}

func (state *watcherState) isReady() bool {
	return state.state == ready
}

func (state *watcherState) isPause() bool {
	return state.state == paused
}

func (state *watcherState) addNotify(event *pdpb.WatchEvent) uint64 {
	return state.q.Add(event)
}

func (state *watcherState) cancelTimeout() {
	if state.hbTimeout != nil {
		state.hbTimeout.Stop()
		state.hbTimeout = nil
	}
}

func (state *watcherState) resetTimeout(timeout time.Duration, fn func(interface{})) {
	state.cancelTimeout()

	t, _ := util.DefaultTimeoutWheel().Schedule(timeout, fn, state.watcher.Addr)
	state.hbTimeout = &t
}

// watcherNotifier is used for notify the newest cell info to all watchers
type watcherNotifier struct {
	sync.RWMutex

	notifies *util.Queue
	pool     *goetty.AddressBasedPool
	watchers map[string]*watcherState
	timeout  time.Duration
}

func newWatcherNotifier(timeout time.Duration) *watcherNotifier {
	wn := &watcherNotifier{
		notifies: util.New(1024),
		watchers: make(map[string]*watcherState),
		timeout:  timeout,
	}

	wn.pool = goetty.NewAddressBasedPool(createConn, wn)
	return wn
}

func (wn *watcherNotifier) start() {
	go func() {
		batch := int64(64)
		items := make([]interface{}, batch, batch)

		for {
			n, err := wn.notifies.Get(int64(batch), items)
			if err != nil {
				log.Infof("stop: watcher notifier stopped")
				return
			}

			for i := int64(0); i < n; i++ {
				nt := items[i].(*notify)

				if !wn.allowNotify(nt.watcher) {
					continue
				}

				if !wn.allowSend(nt) {
					continue
				}

				conn, err := wn.pool.GetConn(nt.watcher)
				if err != nil {
					wn.pause(nt.watcher, true)
					log.Warnf("notify: %d to %s failed, errors:\n%+v",
						nt.offset,
						nt.watcher,
						err)
					continue
				}

				req := &pdpb.WatcherNotify{
					Offset: nt.offset,
				}
				err = conn.WriteAndFlush(req)
				if err != nil {
					wn.pause(nt.watcher, true)
					log.Warnf("notify: %d to %s failed, errors:\n%+v",
						nt.offset,
						nt.watcher,
						err)
					continue
				}
			}
		}
	}()
}

func (wn *watcherNotifier) stop() {
	wn.notifies.Dispose()
}

func (wn *watcherNotifier) removedAllWatcher() {
	wn.Lock()
	for watcher := range wn.watchers {
		wn.removeWatcher(watcher)
	}
	wn.Unlock()
}

// addWatcher add a new watcher for notify the newest cells info.
func (wn *watcherNotifier) addWatcher(watcher pdpb.Watcher) {
	wn.Lock()
	addr := watcher.Addr
	if state, ok := wn.watchers[addr]; ok {
		state.cancelTimeout()
		state.reset()
	} else {
		wn.watchers[addr] = newWatcherState(watcher)
	}
	wn.resetTimeout(addr)

	wn.notifyWithoutLock(wn.watchers[addr], &pdpb.WatchEvent{})
	wn.notifyWithoutLock(wn.watchers[addr], &pdpb.WatchEvent{
		Event: pd.EventInit,
	})
	wn.Unlock()

	log.Infof("notify: %s added", watcher.String())
}

func (wn *watcherNotifier) resetTimeout(addr string) {
	wn.watchers[addr].resetTimeout(wn.timeout, wn.watcherTimeout)
}

// removeWatcher remove a watcher
func (wn *watcherNotifier) removeWatcher(addr string) {
	wn.Lock()
	delete(wn.watchers, addr)
	wn.pool.RemoveConn(addr)
	wn.Unlock()
}

// watcherHeartbeat return true if the watcher resume from pause.
func (wn *watcherNotifier) watcherHeartbeat(addr string, offset uint64) bool {
	wn.Lock()
	value := wn.resume(addr)
	if state, ok := wn.watchers[addr]; ok && !value {
		max := state.q.GetMaxOffset()
		if offset < max {
			wn.notifies.Put(&notify{
				watcher: addr,
				offset:  max,
			})
		}
	}
	wn.Unlock()
	return value
}

func (wn *watcherNotifier) watcherTimeout(arg interface{}) {
	wn.pause(arg.(string), true)
}

func (wn *watcherNotifier) allowNotify(addr string) bool {
	wn.Lock()
	if state, ok := wn.watchers[addr]; ok {
		allow := state.isReady()
		wn.Unlock()
		return allow
	}
	wn.Unlock()
	return false
}

func (wn *watcherNotifier) allowSend(nt *notify) bool {
	wn.Lock()
	if state, ok := wn.watchers[nt.watcher]; ok {
		allow := state.isReady() && state.lastNotifyOffset == 0 || state.lastNotifyOffset < nt.offset
		if allow {
			state.lastNotifyOffset = nt.offset
		}
		wn.Unlock()
		return allow
	}
	wn.Unlock()
	return false
}

func (wn *watcherNotifier) pause(addr string, remove bool) {
	wn.Lock()
	if remove {
		wn.pool.RemoveConn(addr)
	}
	if state, ok := wn.watchers[addr]; ok {
		state.pause()
	}
	wn.Unlock()
}

func (wn *watcherNotifier) resume(addr string) bool {
	if state, ok := wn.watchers[addr]; ok {
		resumeFromPause := state.isPause()

		if state.isPause() {
			state.state = ready
		}

		wn.resetTimeout(addr)
		return resumeFromPause
	}

	return false
}

func (wn *watcherNotifier) notify(event *pdpb.WatchEvent) {
	wn.Lock()
	for _, state := range wn.watchers {
		wn.notifyWithoutLock(state, event)
	}
	wn.Unlock()
}

func (wn *watcherNotifier) notifyWithoutLock(state *watcherState, event *pdpb.WatchEvent) {
	if state.isReady() && pd.MatchEvent(event.Event, state.watcher.EventFlag) {
		nt := &notify{
			watcher: state.watcher.Addr,
			offset:  state.addNotify(event),
		}
		wn.notifies.PutOrUpdate(cmp, nt)
	}
}

func (wn *watcherNotifier) sync(addr string, offset uint64) *pdpb.WatcherNotifyRsp {
	wn.Lock()

	if state, ok := wn.watchers[addr]; ok && state.isReady() {
		items, max := state.q.Get(offset)
		rsp := new(pdpb.WatcherNotifyRsp)
		for _, item := range items {
			rsp.Events = append(rsp.Events, item.(*pdpb.WatchEvent))
		}
		rsp.Offset = max
		wn.Unlock()
		return rsp
	}

	wn.Unlock()
	return nil
}

// ConnectFailed pool status handler
func (wn *watcherNotifier) ConnectFailed(addr string, err error) {
}

// Connected pool status handler
func (wn *watcherNotifier) Connected(addr string, conn goetty.IOSession) {
	go func() {
		for {
			msg, err := conn.Read()
			if err != nil {
				log.Errorf("notify: read from %s failed, errors:\n%+v", addr, err)
				wn.pause(addr, true)
				return
			}

			if s, ok := msg.(*pdpb.WatcherNotifySync); ok {
				err := conn.WriteAndFlush(wn.sync(addr, s.Offset))
				if err != nil {
					wn.pause(addr, true)
					return
				}
			}
		}
	}()
}

func createConn(addr string) goetty.IOSession {
	return goetty.NewConnector(addr,
		goetty.WithClientDecoder(&codec.ProxyDecoder{}),
		goetty.WithClientEncoder(&codec.ProxyEncoder{}))
}
