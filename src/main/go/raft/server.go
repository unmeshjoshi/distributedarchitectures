package raft

import (
	"fmt"
	"sort"
	"sync"
	"time"
)
const (
	Stopped      = "stopped"
	Initialized  = "initialized"
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
	Snapshotting = "snapshotting"
)

const (
	MaxLogEntriesPerRequest         = 2000
	NumberOfLogEntriesAfterSnapshot = 200
)

const (
	// DefaultHeartbeatInterval is the interval that the leader will send
	// AppendEntriesRequests to followers to maintain leadership.
	DefaultHeartbeatInterval = 50 * time.Millisecond

	DefaultElectionTimeout = 150 * time.Millisecond
)

type server struct {
	name        string
	path        string
	state       string
	currentTerm uint64

	votedFor   string
	log        *Log
	leader     string
	mutex      sync.RWMutex
	peers      map[string]*Peer
	syncedPeer map[string]bool
	c          chan *ev

	electionTimeout         time.Duration
	heartbeatInterval       time.Duration
	maxLogEntriesPerRequest uint64
	transporter             Transporter

}

func (s *server) Do(command Command) (interface{}, error) {
	return s.send(command)
}

func NewServer(name string, path string,  connectionString string, transporter Transporter, lookup map[string]*server) (*server, error) {
	s := &server{
		name:                    name,
		path:                    path,
		state:                   Stopped,
		peers:                   make(map[string]*Peer),
		log:                     newLog(),
		c:                       make(chan *ev, 256),
		transporter: transporter,
		heartbeatInterval: time.Second * 1,
	}
	lookup[name] = s
	return s, nil
}

func newLog() *Log {
	return &Log{
		entries: make([]*LogEntry, 0),
	}
}

// Checks if the server is currently running.
func (s *server) Running() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return (s.state != Stopped && s.state != Initialized)
}


// Processes a command.
func (s *server) processCommand(command Command, e *ev) {
	s.debugln("server.command.process")

	// Create an entry for the command in the log.
	entry, err := s.log.createEntry(s.currentTerm, command, e)

	if err != nil {
		s.debugln("server.command.log.entry.error:", err)
		e.c <- err
		return
	}

	if err := s.log.appendEntry(entry); err != nil {
		s.debugln("server.command.log.error:", err)
		e.c <- err
		return
	}

	s.syncedPeer[s.Name()] = true
	if len(s.peers) == 0 {
		commitIndex := s.log.currentIndex()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}


// Sets the state of the server.
func (s *server) setState(state string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Temporarily store previous values.
	//prevState := s.state
	//prevLeader := s.leader

	// Update state and leader.
	s.state = state
	if state == Leader {
		s.leader = s.Name()
		s.syncedPeer = make(map[string]bool)
	}
		//
		//// Dispatch state and leader change events.
		//s.DispatchEvent(newEvent(StateChangeEventType, s.state, prevState))
		//
		//if prevLeader != s.leader {
		//	s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
		//}
}

func (s *server) StartAsLeader() error {
	if err := s.Init(); err != nil {
		return err
	}

	s.log.open(s.path)
	s.setState(Leader)
	debugln(s.GetState())

	go s.leaderLoop()

	return nil
}

func (s *server) StartAsFollower() error {
	if err := s.Init(); err != nil {
		return err
	}

	s.log.open(s.path)
	s.setState(Follower)
	debugln(s.GetState())

	go s.followerLoop()

	return nil
}

func (s *server) Start() error {
	// Exit if the server is already running.
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	if err := s.Init(); err != nil {
		return err
	}

	s.setState(Follower)

	// If no log entries exist then
	// 1. wait for AEs from another node
	// 2. wait for self-join command
	// to set itself promotable
	if !s.promotable() {
		s.debugln("start as a new raft server")

		// If log entries exist then allow promotion to candidate
		// if no AEs received.
	} else {
		s.debugln("start from previous saved state")
	}

	debugln(s.GetState())

	go s.loop()

	return nil
}

// Check if the server is promotable
func (s *server) promotable() bool {
	return s.log.currentIndex() > 0
}

// Get the state of the server for debugging
func (s *server) GetState() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fmt.Sprintf("Name: %s, State: %s, Term: %v, CommitedIndex: %v ", s.name, s.state, s.currentTerm, s.log.commitIndex)
}

// Retrieves the current term of the server.
func (s *server) Term() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.currentTerm
}

// Retrieves the current state of the server.
func (s *server) State() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

func (s *server) loop() {
	defer s.debugln("server.loop.end")
}


// NOP command
type NOPCommand struct {
}

func (c NOPCommand) CommandName() *string {
	m := "raft:nop"
	return &m
}

func (s *server) leaderLoop() {
	logIndex, _ := s.log.lastInfo()

	// Update the peers prevLogIndex to leader's lastLogIndex and start heartbeat.
	s.debugln("leaderLoop.set.PrevIndex to ", logIndex)
	for _, peer := range s.peers {
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}

	// Commit a NOP after the server becomes leader. From the Raft paper:
	// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
	// each server; repeat during idle periods to prevent election timeouts
	// (§5.2)". The heartbeats started above do the "idle" period work.
	go s.Do(NOPCommand{})

	for s.State() == Leader {
		var err error
		select {
		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				s.processCommand(req, e)
				//TODO: For Now Only. We should wait till the command is committed
				continue
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(req)
			}
			// Callback to event.
			e.c <- err
		}
	}
}

func (s *server) followerLoop() {

	for s.State() == Follower {
		var err error

		select {
		case e := <-s.c:
			switch req := e.target.(type) {

			case *AppendEntriesRequest:
				// If heartbeats get too close to the election timeout then send an event.
				update := false
				e.returnValue, update = s.processAppendEntriesRequest(req)
				debugln(e.returnValue, update)
			}
			// Callback to event.
			e.c <- err
		}
	}
}

func (s *server) debugln(v ...interface{}) {
	if logLevel > Debug {
		debugf("[%s Term:%d] %s", s.name, s.Term(), fmt.Sprintln(v...))
	}
}

func (s *server) traceln(v ...interface{}) {
	if logLevel > Trace {
		tracef("[%s] %s", s.name, fmt.Sprintln(v...))
	}
}

func (s *server) Init() error {
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}
	return nil
}

func (s *server) Name() string {
	return s.name
}


// Sends an event to the event loop to be processed. The function will wait
// until the event is actually processed before returning.
func (s *server) send(value interface{}) (interface{}, error) {
	event := &ev{target: value, c: make(chan error, 1)}
	s.c <- event
	err := <-event.c
	return event.returnValue, err
}



func (s *server) sendAsync(value interface{}) {
	event := &ev{target: value, c: make(chan error, 1)}
	// try a non-blocking send first
	// in most cases, this should not be blocking
	// avoid create unnecessary go routines
	select {
	case s.c <- event:
		return
	default:
	}

	go func() {
		s.c <- event
	}()
}


// Adds a peer to the server.
func (s *server) AddPeer(name string, connectiongString string) error {
	s.debugln("server.peer.add: ", name, len(s.peers))

	// Do not allow peers to be added twice.
	if s.peers[name] != nil {
		return nil
	}

	// Skip the Peer if it has the same name as the Server
	if s.name != name {
		peer := newPeer(s, name, connectiongString, s.heartbeatInterval)

		if s.State() == Leader {
			peer.startHeartbeat()
		}

		s.peers[peer.Name] = peer

		//s.DispatchEvent(newEvent(AddPeerEventType, name, nil))
	}

	// Write the configuration to file.
	//s.writeConf()

	return nil
}


// Retrieves the object that transports requests.
func (s *server) Transporter() Transporter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.transporter
}


// Appends zero or more log entry from the leader to this server.
func (s *server) AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	var ret, _ = s.send(req)
	resp, _ := ret.(*AppendEntriesResponse)
	return resp
}

// Processes the "append entries" request.
func (s *server) processAppendEntriesRequest(req *AppendEntriesRequest) (*AppendEntriesResponse, bool) {
	s.traceln("server.ae.process")

	if req.Term < s.currentTerm {
		s.debugln("server.ae.error: stale term")
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), false
	}

	if req.Term == s.currentTerm {
		// change state to follower
		s.state = Follower
		// discover new leader when candidate
		// save leader name when follower
		s.leader = req.LeaderName
	} else {
		// Update term and leader.
		s.updateCurrentTerm(req.Term, req.LeaderName)
	}

	// Reject if log doesn't contain a matching previous entry.
	if err := s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		s.debugln("server.ae.truncate.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// Append entries to the log.
	if err := s.log.appendEntries(req.Entries); err != nil {
		s.debugln("server.ae.append.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// Commit up to the commit index.
	if err := s.log.setCommitIndex(req.CommitIndex); err != nil {
		s.debugln("server.ae.commit.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// once the server appended and committed all the log entries from the leader

	return newAppendEntriesResponse(s.currentTerm, true, s.log.currentIndex(), s.log.CommitIndex()), true
}


// updates the current term for the server. This is only used when a larger
// external term is found.
func (s *server) updateCurrentTerm(term uint64, leaderName string) {
	//_assert(term > s.currentTerm,
	//	"upadteCurrentTerm: update is called when term is not larger than currentTerm")

	s.mutex.Lock()
	defer s.mutex.Unlock()
	// Store previous values temporarily.
	//prevTerm := s.currentTerm
	//prevLeader := s.leader

	// set currentTerm = T, convert to follower (§5.1)
	// stop heartbeats before step-down
	if s.state == Leader {
		s.mutex.Unlock()
		for _, peer := range s.peers {
			peer.stopHeartbeat(false)
		}
		s.mutex.Lock()
	}
	// update the term and clear vote for
	if s.state != Follower {
		s.mutex.Unlock()
		s.setState(Follower)
		s.mutex.Lock()
	}
	s.currentTerm = term
	s.leader = leaderName
	s.votedFor = ""
}


// Processes the "append entries" response from the peer. This is only
// processed when the server is a leader. Responses received during other
// states are dropped.
func (s *server) processAppendEntriesResponse(resp *AppendEntriesResponse) {
	// If we find a higher term then change to a follower and exit.
	if resp.Term() > s.Term() {
		s.updateCurrentTerm(resp.Term(), "")
		return
	}

	// panic response if it's not successful.
	if !resp.Success() {
		return
	}

	// if one peer successfully append a log from the leader term,
	// we add it to the synced list
	if resp.append == true {
		fmt.Println(s.syncedPeer)
		fmt.Println(resp.peer, "->", s.syncedPeer[resp.peer])
		s.syncedPeer[resp.peer] = true
		fmt.Println(resp.peer, "->", s.syncedPeer[resp.peer])
	}

	// Increment the commit count to make sure we have a quorum before committing.
	if len(s.syncedPeer) < s.QuorumSize() {
		return
	}

	// Determine the committed index that a majority has.
	var indices []uint64
	indices = append(indices, s.log.currentIndex())
	for _, peer := range s.peers {
		indices = append(indices, peer.getPrevLogIndex())
	}
	sort.Sort(sort.Reverse(uint64Slice(indices)))

	// We can commit up to the index which the majority of the members have appended.
	commitIndex := indices[s.QuorumSize()-1]
	committedIndex := s.log.commitIndex

	if commitIndex > committedIndex {
		// leader needs to do a fsync before committing log entries
		s.log.sync()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}


// Retrieves the number of member servers in the consensus.
func (s *server) MemberCount() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.peers) + 1
}

// Retrieves the number of servers required to make a quorum.
func (s *server) QuorumSize() int {
	return (s.MemberCount() / 2) + 1
}

// Creates a new peer.
func newPeer(server *server, name string, connectionString string, heartbeatInterval time.Duration) *Peer {
	return &Peer{
		server:            server,
		Name:              name,
		ConnectionString:  connectionString,
		heartbeatInterval: heartbeatInterval,
	}
}
