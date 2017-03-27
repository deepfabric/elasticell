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

package raft

import (
	"fmt"

	"os"
	"strconv"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/deepfabric/elasticell/pkg/log"
	"golang.org/x/net/context"
)

var (
	defaultSnapCount        uint64 = 10000
	snapshotCatchUpEntriesN uint64 = 10000
)

// RawNode is the raw raft node
type RawNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- interface{}       // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() ([]byte, error)
	lastIndex   uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport

	stopc chan struct{} // signals proposal channel closed
}

// NewRawNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRawNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan interface{}, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan interface{})
	errorC := make(chan error)

	rn := &RawNode{
		proposeC:         proposeC,
		confChangeC:      confChangeC,
		commitC:          commitC,
		errorC:           errorC,
		id:               id,
		peers:            peers,
		join:             join,
		waldir:           fmt.Sprintf("raftexample-%d", id),
		snapdir:          fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot:      getSnapshot,
		snapCount:        defaultSnapCount,
		stopc:            make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rn.startRaft()
	return commitC, errorC, rn.snapshotterReady
}

func (rn *RawNode) startRaft() {
	if !fileutil.Exist(rn.snapdir) {
		if err := os.Mkdir(rn.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rn.snapshotter = snap.New(rn.snapdir)
	rn.snapshotterReady <- rn.snapshotter

	oldwal := wal.Exist(rn.waldir)
	rn.wal = rn.replayWAL()

	rpeers := make([]raft.Peer, len(rn.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:              uint64(rn.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		rn.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rn.join {
			startPeers = nil
		}
		rn.node = raft.StartNode(c, startPeers)
	}

	ss := &stats.ServerStats{}
	ss.Initialize()

	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rn.id)),
		ErrorC:      make(chan error),
	}

	rn.transport.Start()
	for i := range rn.peers {
		if i+1 != rn.id {
			rn.transport.AddPeer(types.ID(i+1), []string{rn.peers[i]})
		}
	}

	go rn.serveRaft()
	go rn.serveChannels()
}

// openWAL returns a WAL ready for reading.
func (rn *RawNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rn.waldir) {
		if err := os.Mkdir(rn.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rn.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	// log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(rn.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rn *RawNode) replayWAL() *wal.WAL {
	// log.Printf("replaying WAL of member %d", rn.id)
	snapshot := rn.loadSnapshot()
	w := rn.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rn.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rn.raftStorage.ApplySnapshot(*snapshot)
	}
	rn.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rn.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rn.lastIndex = ents[len(ents)-1].Index
	} else {
		rn.commitC <- nil
	}
	return w
}

func (rn *RawNode) writeError(err error) {
	close(rn.commitC)
	rn.errorC <- err
	close(rn.errorC)
	rn.node.Stop()
}

func (rn *RawNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rn.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// stop closes http, closes all channels, and stops raft.
func (rn *RawNode) stop() {
	close(rn.commitC)
	close(rn.errorC)
	rn.node.Stop()
}

func (rn *RawNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	// log.Printf("publishing snapshot at index %d", rn.snapshotIndex)
	// defer log.Printf("finished publishing snapshot at index %d", rn.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rn.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rn.appliedIndex)
	}
	rn.commitC <- nil // trigger kvstore to load snapshot

	rn.confState = snapshotToSave.Metadata.ConfState
	rn.snapshotIndex = snapshotToSave.Metadata.Index
	rn.appliedIndex = snapshotToSave.Metadata.Index
}

func (rn *RawNode) maybeTriggerSnapshot() {
	if rn.appliedIndex-rn.snapshotIndex <= rn.snapCount {
		return
	}

	// log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rn.appliedIndex, rn.snapshotIndex)
	data, err := rn.getSnapshot()
	if err != nil {
		// log.Panic(err)
	}
	snap, err := rn.raftStorage.CreateSnapshot(rn.appliedIndex, &rn.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rn.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rn.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rn.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rn.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	// log.Printf("compacted log at index %d", compactIndex)
	rn.snapshotIndex = rn.appliedIndex
}

func (rn *RawNode) serveChannels() {
	snap, err := rn.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rn.confState = snap.Metadata.ConfState
	rn.snapshotIndex = snap.Metadata.Index
	rn.appliedIndex = snap.Metadata.Index

	defer rn.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64

		for rn.proposeC != nil && rn.confChangeC != nil {
			select {
			case prop, ok := <-rn.proposeC:
				if !ok {
					rn.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rn.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rn.confChangeC:
				if !ok {
					rn.confChangeC = nil
				} else {
					confChangeCount += 1
					cc.ID = confChangeCount
					rn.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rn.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rn.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rn.node.Ready():
			rn.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.saveSnap(rd.Snapshot)
				rn.raftStorage.ApplySnapshot(rd.Snapshot)
				rn.publishSnapshot(rd.Snapshot)
			}
			rn.raftStorage.Append(rd.Entries)
			rn.transport.Send(rd.Messages)
			if ok := rn.publishEntries(rn.entriesToApply(rd.CommittedEntries)); !ok {
				rn.stop()
				return
			}
			rn.maybeTriggerSnapshot()
			rn.node.Advance()

		case err := <-rn.transport.ErrorC:
			rn.writeError(err)
			return

		case <-rn.stopc:
			rn.stop()
			return
		}
	}
}

func (rn *RawNode) serveRaft() {
	// url, err := url.Parse(rc.peers[rc.id-1])
	// if err != nil {
	// 	log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	// }

	// ln, err := newStoppableListener(url.Host, rc.httpstopc)
	// if err != nil {
	// 	log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	// }

	// err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	// select {
	// case <-rc.httpstopc:
	// default:
	// 	log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	// }
	// close(rc.httpdonec)
}

func (rn *RawNode) saveSnap(snap raftpb.Snapshot) error {
	if err := rn.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rn.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rn.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rn *RawNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rn.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rn.appliedIndex)
	}
	if rn.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rn.appliedIndex-firstIdx+1:]
	}
	return
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rn *RawNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case rn.commitC <- &s:
			case <-rn.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rn.confState = *rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					// log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rn.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rn.lastIndex {
			select {
			case rn.commitC <- nil:
			case <-rn.stopc:
				return false
			}
		}
	}
	return true
}

func (rn *RawNode) Process(ctx context.Context, m raftpb.Message) error {
	return rn.node.Step(ctx, m)
}

func (rn *RawNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rn *RawNode) ReportUnreachable(id uint64) {

}

func (rn *RawNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {

}
