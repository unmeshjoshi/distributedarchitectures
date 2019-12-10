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
}
