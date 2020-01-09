package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestLogRecovery(t *testing.T) {
	tmpLog := newLog()
	e0, _ := newLogEntry(tmpLog, nil, 1, 1, &testCommand{X: "foo"})
	e1, _ := newLogEntry(tmpLog, nil, 2, 1, &testCommand{X: "bar"})
	f, _ := ioutil.TempFile("", "raft-log-")

	e0.WriteTo(f)
	e1.WriteTo(f)
	f.WriteString("CORRUPT!")
	f.Close()

	log := newLog()

	if err := log.open(f.Name()); err != nil {
		t.Fatalf("Unable to open log: %v", err)
	}

	entry := log.entries[0]
	r := bytes.NewReader(entry.pb.Command)
	c := testCommand{}
	err := json.NewDecoder(r).Decode(&c)
	if (err != nil) {
		t.Fail()
	}
	fmt.Println(c)
}