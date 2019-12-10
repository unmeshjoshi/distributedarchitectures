package raft

import (
	"fmt"
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

	votedFor string
	log      *Log
	leader   string
	mutex    sync.RWMutex
	peers      map[string]*Peer
	syncedPeer map[string]bool
	c                 chan *ev
}

func (s *server) Do(command Command) (interface{}, error) {
	return s.send(command)
}

func NewServer(name string, path string,  connectionString string) (*server, error) {
	s := &server{
		name:                    name,
		path:                    path,
		state:                   Stopped,
		peers:                   make(map[string]*Peer),
		log:                     newLog(),
		c:                       make(chan *ev, 256),
	}
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

func (s *server) leaderLoop() {
	for s.State() == Leader {
		var err error
		select {
		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				s.processCommand(req, e)
				e.c <- err //TODO: For Now Only. We should wait till the command is committed
				continue
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
