package kvstore

import (
	"consensus/kvstore/embed"
	"consensus/kvstore/types"
	raft "consensus/newraft"
	"consensus/newraft/raftpb"
	"consensus/newraft/wal"
	"github.com/prometheus/common/log"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"
)



type config struct {
	ec           embed.Config
}

type raftNode struct {
	tickMu *sync.Mutex
	lg     *zap.Logger

	raft.Node
	raftStorage *raft.MemoryStorage
	storage     Storage
	heartbeat   time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	//transport rafthttp.Transporter

	// a chan to send/receive snapshot
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	applyc chan apply

	// utility
	ticker  *time.Ticker
	stopped chan struct{}
	done    chan struct{}

}

type apply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
}

type Server struct {
	// inflightSnapshots holds count the number of snapshots currently inflight.
	inflightSnapshots int64  // must use atomic operations to access; keep 64-bit aligned.
	appliedIndex      uint64 // must use atomic operations to access; keep 64-bit aligned.
	committedIndex    uint64 // must use atomic operations to access; keep 64-bit aligned.
	term              uint64 // must use atomic operations to access; keep 64-bit aligned.
	lead              uint64 // must use atomic operations to access; keep 64-bit aligned.

	// consistIndex used to hold the offset of current executing entry
	// It is initialized to 0 before executing any entry.
	//consistIndex consistentIndex // must use atomic operations to access; keep 64-bit aligned.
	r raftNode // uses 64-bit atomics; keep 64-bit aligned.

	readych chan struct{}
	id         types.ID
}

func (s *Server) setCommittedIndex(v uint64) {
	atomic.StoreUint64(&s.committedIndex, v)
}

func (s *Server) getCommittedIndex() uint64 {
	return atomic.LoadUint64(&s.committedIndex)
}

func (s *Server) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
}

func (s *Server) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *Server) setTerm(v uint64) {
	atomic.StoreUint64(&s.term, v)
}

func (s *Server) getTerm() uint64 {
	return atomic.LoadUint64(&s.term)
}


func (s *Server) setLead(v uint64) {
	atomic.StoreUint64(&s.lead, v)
}

func (s *Server) getLead() uint64 {
	return atomic.LoadUint64(&s.lead)
}


func (s *Server) ID() types.ID { return s.id }

func (s *Server) Leader() types.ID { return types.ID(s.getLead()) }

func (s *Server) Lead() uint64 { return s.getLead() }

func (s *Server) CommittedIndex() uint64 { return s.getCommittedIndex() }

func (s *Server) AppliedIndex() uint64 { return s.getAppliedIndex() }

func (s *Server) Term() uint64 { return s.getTerm() }
func (s *Server) isLeader() bool {
	return uint64(s.ID()) == s.Lead()
}

func NewServer() (srv *Server, err error) {
	srv = &Server{
		r:                 *NewRaftNode(),
		id:                0,
	}
	return srv, nil
}

func NewRaftNode() *raftNode {


	var maxInFlightMsgSnap = 16
	var cfg embed.Config = config{*embed.NewConfig()}.ec
	s := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              uint64(1),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:s,
		Members: map[types.ID]*raft.Member{
			1:   {ID: 1},
			20:  {ID: 20},
			100: {ID: 100},
			5:   {ID: 5},
			50:  {ID: 50},
		},
		Logger:                     raft.RaftLogger(),
	}

	var ids []types.ID
	for _, m := range c.Members {
		ids = append(ids, m.ID)
	}

	peers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		var ctx []byte
		peers[i] = raft.Peer{ID: uint64(id), Context: ctx}
	}

	var n = raft.StartNode(c, peers)

	p, _ := ioutil.TempDir(os.TempDir(), "waltest")
	w, _ := wal.Create(zap.NewExample(), p, []byte("somedata"))

	r := &raftNode{
		tickMu:     new(sync.Mutex),
		lg:         cfg.Logger,
		heartbeat : time.Duration(cfg.TickMs) * time.Millisecond,
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan apply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
		Node:        n,
		storage:NewStorage(w, nil),
		raftStorage:raft.NewMemoryStorage(),

	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}
	return r
}

func (s *Server) run() {
	rh := &raftReadyHandler{
		getLead:    func() (lead uint64) { return s.getLead() },
		updateLead: func(lead uint64) { s.setLead(lead) },
		updateLeadership: func(newLeader bool) {
			println("UpdateLeadership")
		},
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
	s.r.start(rh)

	for {
		select {
		case ap := <-s.r.applyc:
			log.Info(ap)
			s.applyAll(&ap)
			//f := func(context.Context) { s.applyAll(&ep, &ap) }
			//sched.Schedule(f)
		//case leases := <-expiredLeaseC:
		//	s.goAttach(func() {
		//		// Increases throughput of expired leases deletion process through parallelization
		//		c := make(chan struct{}, maxPendingRevokes)
		//		for _, lease := range leases {
		//			select {
		//			case c <- struct{}{}:
		//			case <-s.stopping:
		//				return
		//			}
		//			lid := lease.ID
		//			s.goAttach(func() {
		//				ctx := s.authStore.WithRoot(s.ctx)
		//				_, lerr := s.LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: int64(lid)})
		//				if lerr == nil {
		//					leaseExpired.Inc()
		//				} else {
		//					if lg != nil {
		//						lg.Warn(
		//							"failed to revoke lease",
		//							zap.String("lease-id", fmt.Sprintf("%016x", lid)),
		//							zap.Error(lerr),
		//						)
		//					} else {
		//						plog.Warningf("failed to revoke %016x (%q)", lid, lerr.Error())
		//					}
		//				}
		//
		//				<-c
		//			})
		//		}
		//	})
		//case err := <-s.errorc:
		//	if lg != nil {
		//		lg.Warn("server error", zap.Error(err))
		//		lg.Warn("data-dir used by this member must be removed")
		//	} else {
		//		plog.Errorf("%s", err)
		//		plog.Infof("the data-dir used by this member must be removed.")
		//	}
		//	return
		//case <-getSyncC():
		//	if s.v2store.HasTTLKeys() {
		//		s.sync(s.Cfg.ReqTimeout())
		//	}
		//case <-s.stop:
		//	return
		}
	}
}


// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

type raftReadyHandler struct {
	getLead              func() (lead uint64)
	updateLead           func(lead uint64)
	updateLeadership     func(newLeader bool)
	updateCommittedIndex func(uint64)
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {

	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		islead := false

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					rh.updateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					rh.updateLeadership(newLeader)
					//r.td.Reset()
				}

				if len(rd.ReadStates) != 0 {
					select {
					//case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						if r.lg != nil {
							r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
						} else {
							plog.Warningf("timed out sending read state")
						}
					case <-r.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				ap := apply{
					entries:  rd.CommittedEntries,
					snapshot: rd.Snapshot,
					notifyc:  notifyc,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					//r.transport.Send(r.processMessages(rd.Messages))
					println("Applying messages")
					println(r.processMessages(rd.Messages))
				}

				// gofail: var raftBeforeSave struct{}
				if err := r.storage.Save(rd.HardState, rd.Entries); err != nil {
					if r.lg != nil {
						r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
					} else {
						plog.Fatalf("raft save state and entries error: %v", err)
					}
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
						if r.lg != nil {
							r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
						} else {
							plog.Fatalf("raft save snapshot error: %v", err)
						}
					}
					// Server now claim the snapshot has been persisted onto the disk
					notifyc <- struct{}{}

					// gofail: var raftAfterSaveSnap struct{}
					r.raftStorage.ApplySnapshot(rd.Snapshot)
					if r.lg != nil {
						r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					} else {
						plog.Infof("raft applied incoming snapshot at index %d", rd.Snapshot.Metadata.Index)
					}
					// gofail: var raftAfterApplySnap struct{}
				}

				r.raftStorage.Append(rd.Entries)

				if !islead {
					// finish processing incoming messages before we signal raftdone chan
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before apply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := true
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					//r.transport.Send(msgs)
					println("Sending messages")
					println(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				r.Advance()
			case <-r.stopped:
				return
			}
		}
	}()
}


func updateCommittedIndex(ap *apply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

func (r *raftNode) onStop() {


}


func (s *Server) applyAll(apply *apply) {


	// wait for the raft routine to finish the disk writes before triggering a
	// snapshot. or applied index might be greater than the last index in raft
	// storage, since the raft routine might be slower than apply routine.
	<-apply.notifyc

}


func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			//ok, exceed := r.td.Observe(ms[i].To)
			//if !ok {
			//	// TODO: limit request rate.
			//	if r.lg != nil {
			//		r.lg.Warn(
			//			"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
			//			zap.String("to", fmt.Sprintf("%x", ms[i].To)),
			//			zap.Duration("heartbeat-interval", r.heartbeat),
			//			zap.Duration("expected-duration", 2*r.heartbeat),
			//			zap.Duration("exceeded-duration", exceed),
			//		)
			//	} else {
			//		plog.Warningf("failed to send out heartbeat on time (exceeded the %v timeout for %v, to %x)", r.heartbeat, exceed, ms[i].To)
			//		plog.Warningf("server is likely overloaded")
			//	}
			//	heartbeatSendFailures.Inc()
			//}
		}
	}
	return ms
}

func (r *raftNode) isIDRemoved(to uint64) bool {
	return false
}
