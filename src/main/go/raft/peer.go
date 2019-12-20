package raft

import (
	"sync"
	"time"
)

type Peer struct {
	server            *server
	Name              string `json:"name"`
	ConnectionString  string `json:"connectionString"`
	prevLogIndex      uint64
	mutex             sync.RWMutex
	stopChan          chan bool
	heartbeatInterval time.Duration
	lastActivity      time.Time
}

// Starts the peer heartbeat.
func (p *Peer) startHeartbeat() {
	p.stopChan = make(chan bool)
	c := make(chan bool)
	go p.heartbeat(c)
	<-c
}


// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat(c chan bool) {
	c <- true

	ticker := time.Tick(p.heartbeatInterval)

	debugln("peer.heartbeat: ", p.Name, p.heartbeatInterval)

	for {
		select {
			case <-ticker:
			start := time.Now()
			p.flush()
			duration := time.Now().Sub(start)
			debugln("Flush time:", duration)
		}
	}
}

func (p *Peer) flush() {
	debugln("peer.heartbeat.flush: ", p.Name)
	prevLogIndex := p.getPrevLogIndex()
	term := p.server.currentTerm

	entries, prevLogTerm := p.server.log.getEntriesAfter(prevLogIndex, p.server.maxLogEntriesPerRequest)

	if entries != nil {
		p.sendAppendEntriesRequest(newAppendEntriesRequest(term, prevLogIndex, prevLogTerm, p.server.log.CommitIndex(), p.server.name, entries))
	}
}


// Retrieves the previous log index.
func (p *Peer) getPrevLogIndex() uint64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.prevLogIndex
}

// Sets the previous log index.
func (p *Peer) setPrevLogIndex(value uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.prevLogIndex = value
}


// Sends an AppendEntries request to the peer through the transport.
func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
	tracef("peer.append.send: %s->%s [prevLog:%v length: %v]\n",
		p.server.Name(), p.Name, req.PrevLogIndex, len(req.Entries))

	var resp *AppendEntriesResponse = p.server.Transporter().SendAppendEntriesRequest(p.server, p, req)
	if resp == nil {

		debugln("peer.append.timeout: ", p.server.Name(), "->", p.Name)
		return
	}
	traceln("peer.append.resp: ", p.server.Name(), "<-", p.Name)

	// If successful then update the previous log index.
	p.mutex.Lock()
	p.lastActivity = time.Now()
	if resp.Success() {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].GetIndex()

			// if peer append a log entry from the current term
			// we set append to true
			if req.Entries[len(req.Entries)-1].GetTerm() == p.server.currentTerm {
				resp.append = true
			}
		}
		traceln("peer.append.resp.success: ", p.Name, "; idx =", p.prevLogIndex)
		// If it was unsuccessful then decrement the previous log index and
		// we'll try again next time.
	} else {
		if resp.Term() > p.server.Term() {
			// this happens when there is a new leader comes up that this *leader* has not
			// known yet.
			// this server can know until the new leader send a ae with higher term
			// or this server finish processing this response.
			debugln("peer.append.resp.not.update: new.leader.found")
		} else if resp.Term() == req.Term && resp.CommitIndex() >= p.prevLogIndex {
			// we may miss a response from peer
			// so maybe the peer has committed the logs we just sent
			// but we did not receive the successful reply and did not increase
			// the prevLogIndex

			// peer failed to truncate the log and sent a fail reply at this time
			// we just need to update peer's prevLog index to commitIndex

			p.prevLogIndex = resp.CommitIndex()
			debugln("peer.append.resp.update: ", p.Name, "; idx =", p.prevLogIndex)

		} else if p.prevLogIndex > 0 {
			// Decrement the previous log index down until we find a match. Don't
			// let it go below where the peer's commit index is though. That's a
			// problem.
			p.prevLogIndex--
			// if it not enough, we directly decrease to the index of the
			if p.prevLogIndex > resp.Index() {
				p.prevLogIndex = resp.Index()
			}

			debugln("peer.append.resp.decrement: ", p.Name, "; idx =", p.prevLogIndex)
		}
	}
	p.mutex.Unlock()

	// Attach the peer to resp, thus server can know where it comes from
	resp.peer = p.Name
	// Send response to server for processing.
	p.server.sendAsync(resp)
}


// Stops the peer heartbeat.
func (p *Peer) stopHeartbeat(flush bool) {
	p.stopChan <- flush
}