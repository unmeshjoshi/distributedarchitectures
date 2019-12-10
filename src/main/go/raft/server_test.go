package raft

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestAppendLogThroughChannelIncrementsIndexSequentially(t *testing.T) {

	s1, _ := NewServer("1", createTempLogDir(t), "http://121.0.0.1:8000")
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

func (t *testCommand) CommandName() string {
	return "testCommand"
}

func submitRequests(s1 *server) {

}
