package raft

import (
	"consensus/raft/raftpb"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"io"
)

type LogEntry struct {
	pb		*raftpb.LogEntry
	Position	int64	// position in the log file
	log		*Log
	event		*ev
}

// An internal event to be processed by the server's event loop.
type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan error
}



func (e *LogEntry) Index() uint64 {
	return e.pb.GetIndex()
}

func (e *LogEntry) Term() uint64 {
	return e.pb.GetTerm()
}

func (e *LogEntry) CommandName() string {
	return e.pb.GetCommandName()
}

func (e *LogEntry) Command() []byte {
	return e.pb.GetCommand()
}

// Encodes the log entry to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (e *LogEntry) WriteTo(w io.Writer) (int, error) {
	b, err := proto.Marshal(e.pb)
	if err != nil {
		return -1, err
	}

	if _, err = fmt.Fprintf(w, "%8x\n", len(b)); err != nil {
		return -1, err
	}

	return w.Write(b)
}

// Decodes the log entry from a buffer. Returns the number of bytes read and
// any error that occurs.
func (e *LogEntry) ReadFrom(r io.Reader) (int, error) {

	var length int
	_, err := fmt.Fscanf(r, "%8x\n", &length)
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(r, data)

	if err != nil {
		return -1, err
	}

	if err = proto.Unmarshal(data, e.pb); err != nil {
		return -1, err
	}

	return length + 8 + 1, nil
}
