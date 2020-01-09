package raft

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestAppendLogThroughChannelIncrementsIndexSequentially(t *testing.T) {

	s1, _ := NewServer("1", createTempLogDir(t), "http://121.0.0.1:8000", &testTransporter{}, map[string]*server{})
	s1.StartAsLeader()

	go s1.Do(&testCommand{X: "test1"})
	go s1.Do(&testCommand{X: "test2"})

	waitForLogEntries(s1)

	firstEntry := s1.log.entries[0]
	assert.Equal(t, uint64(1), firstEntry.Index())

	secondEntry := s1.log.entries[1]
	assert.Equal(t, uint64(2), secondEntry.Index())
}

func waitForLogEntries(s1 *server) {
	for {
		if len(s1.log.entries) == 2 {
			break
		}
	}
}

type testCommand struct {
	X string
}

func createTempLogDir(t *testing.T) string {
	tmpdirpath, err := ioutil.TempDir(os.TempDir(), "raftlog")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirpath)

	if Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			t.Fatal(err)
		}

	}
	return tmpdirpath
}

func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (t *testCommand) CommandName() *string {
	commandName := t.X
	return &commandName
}

func submitRequests(s1 *server) {

}

func TestLeaderAppendEntriesWithEmptyLog(t *testing.T) {
	lookup := map[string]*server{}
	transporter := &testTransporter{}

	transporter.sendAppendEntriesRequestFunc = func(s *server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return lookup[peer.Name].AppendEntries(req)
	}

	s1, _ := NewServer("1", createTempLogDir(t), "http://121.0.0.1:8000", transporter,lookup)
	s2, _ := NewServer("2", createTempLogDir(t), "http://121.0.0.1:8001", transporter,lookup)
	s3, _ := NewServer("3", createTempLogDir(t), "http://121.0.0.1:8002",transporter, lookup)

	s1.AddPeer("2", "")
	s1.AddPeer("3", "")

	//assume s1 is leader
	//add to s1 log
	//flush s1 log
	s1.StartAsLeader()
	s2.StartAsFollower()
	s3.StartAsFollower()

	//send few commands which get replicated on all the servers
	sendCommand(s1, &testCommand{X: "test1"}, t)
	sendCommand(s1, &testCommand{X: "test2"}, t)
	sendCommand(s1, &testCommand{X: "test3"}, t)
	sendCommand(s1, &testCommand{X: "test4"}, t)

	assert.Equal(t, s1.log.currentIndex(), uint64(5))
	assert.Equal(t, s2.log.currentIndex(), uint64(5))
	assert.Equal(t, s3.log.currentIndex(), uint64(5))

	assert.Equal(t, s1.log.commitIndex, uint64(5)) //first is a no-op command send by leaders to followers after it starts working as leader
}

func sendCommand(s *server, c Command, t *testing.T) {
	send, err := s.send(c)
	assert.Equal(t, err, nil)
	assert.Equal(t, send, "")
}


type testTransporter struct {
	sendAppendEntriesRequestFunc func(server *server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse
}


func (t *testTransporter) SendAppendEntriesRequest(server *server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
	return t.sendAppendEntriesRequestFunc(server, peer, req)
}
