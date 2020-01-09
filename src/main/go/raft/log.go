package raft

import (
	"bufio"
	"bytes"
	"consensus/raft/raftpb"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type Log struct {
	file		*os.File
	path		string
	entries		[]*LogEntry
	commitIndex	uint64
	mutex		sync.RWMutex
	startIndex	uint64	// the index before the first entry in the Log entries
	startTerm	uint64
}

// Opens the log file and reads existing entries. The log can remain open and
// continue to append entries to the end of the log.
func (l *Log) open(path string) error {
	// Read all the entries from the log if one exists.
	var readBytes int64

	var err error
	debugln("log.open.open ", path)
	// open log file
	l.file, err = os.OpenFile(path, os.O_RDWR, 0600)
	l.path = path

	if err != nil {
		// if the log file does not exist before
		// we create the log file and set commitIndex to 0
		if os.IsNotExist(err) {
			l.file, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
			debugln("log.open.create ", path)

			return err
		}
		return err
	}
	debugln("log.open.exist ", path)

	// Read the file and decode entries.
	for {
		// Instantiate log entry and decode into it.
		entry, _ := newLogEntry(l, nil, 0, 0, nil)
		entry.Position, _ = l.file.Seek(0, os.SEEK_CUR)

		n, err := entry.ReadFrom(l.file)
		if err != nil {
			if err == io.EOF {
				debugln("open.log.append: finish ")
			} else {
				if err = os.Truncate(path, readBytes); err != nil {
					return fmt.Errorf("raft.Log: Unable to recover: %v", err)
				}
			}
			break
		}
		if entry.Index() > l.startIndex {
			// Append entry.
			l.entries = append(l.entries, entry)
			if entry.Index() <= l.commitIndex {
				//command, err := newCommand(entry.CommandName(), entry.Command())
				if err != nil {
					continue
				}
				//l.ApplyFunc(entry, command)
			}
			debugln("open.log.append log index ", entry.Index())
		}

		readBytes += int64(n)
	}
	debugln("open.log.recovery number of log ", len(l.entries))
	return nil
}


// The current index in the log.
func (l *Log) currentIndex() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.internalCurrentIndex()
}


// The current index in the log without locking
func (l *Log) internalCurrentIndex() uint64 {
	if len(l.entries) == 0 {
		return l.startIndex
	}
	return l.entries[len(l.entries)-1].Index()
}


// Creates a log entry associated with this log.
func (l *Log) createEntry(term uint64, command Command, e *ev) (*LogEntry, error) {
	return newLogEntry(l, e, l.nextIndex(), term, command)
}


// The next index in the log.
func (l *Log) nextIndex() uint64 {
	return l.currentIndex() + 1
}


// Writes a single log entry to the end of the log.
func (l *Log) appendEntry(entry *LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.file == nil {
		return errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if l.hasEntries() {
		lastEntry := l.lastEntry()
		if entry.Term() < lastEntry.Term() {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		} else if entry.Term() == lastEntry.Term() && entry.Index() <= lastEntry.Index() {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}
	}

	position, _ := l.file.Seek(0, os.SEEK_CUR)

	entry.Position = position

	// Write to storage.
	if _, err := entry.WriteTo(l.file); err != nil {
		return err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return nil
}

func (l *Log) lastEntry() *LogEntry {
	lastEntry := l.entries[len(l.entries)-1]
	return lastEntry
}

func (l *Log) hasEntries() bool {
	return len(l.entries) > 0
}


// Updates the commit index and writes entries after that index to the stable storage.
func (l *Log) setCommitIndex(index uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// this is not error any more after limited the number of sending entries
	// commit up to what we already have
	if index > l.startIndex+uint64(len(l.entries)) {
		debugln("raft.Log: Commit index", index, "set back to ", len(l.entries))
		index = l.startIndex + uint64(len(l.entries))
	}

	// Do not allow previous indices to be committed again.

	// This could happens, since the guarantee is that the new leader has up-to-dated
	// log entries rather than has most up-to-dated committed index

	// For example, Leader 1 send log 80 to follower 2 and follower 3
	// follower 2 and follow 3 all got the new entries and reply
	// leader 1 committed entry 80 and send reply to follower 2 and follower3
	// follower 2 receive the new committed index and update committed index to 80
	// leader 1 fail to send the committed index to follower 3
	// follower 3 promote to leader (server 1 and server 2 will vote, since leader 3
	// has up-to-dated the entries)
	// when new leader 3 send heartbeat with committed index = 0 to follower 2,
	// follower 2 should reply success and let leader 3 update the committed index to 80

	if index < l.commitIndex {
		return nil
	}

	// Find all entries whose index is between the previous index and the current index.
	for i := l.commitIndex + 1; i <= index; i++ {
		entryIndex := i - 1 - l.startIndex
		entry := l.entries[entryIndex]

		// Update commit index.
		l.commitIndex = entry.Index()

		// ReadFrom the command.
		command:= entry.Command()

		// Apply the changes to the state machine and store the error code.
		returnValue, err := l.ApplyFunc(entry, command)

		debugf("setCommitIndex.set.result index: %v, entries index: %v", i, entryIndex)
		if entry.event != nil {
			entry.event.returnValue = returnValue
			entry.event.c <- err
		}
	}
	return nil
}

func (l *Log) ApplyFunc(entry *LogEntry, command []byte) (interface{}, error){
	fmt.Println("Applying ", entry)
	return "", nil
}


// Retrieves a list of entries after a given index as well as the term of the
// index provided. A nil list of entries is returned if the index no longer
// exists because a snapshot was made.
func (l *Log) getEntriesAfter(index uint64, maxLogEntriesPerRequest uint64) ([]*LogEntry, uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// Return nil if index is before the start of the log.
	if index < l.startIndex {
		traceln("log.entriesAfter.before: ", index, " ", l.startIndex)
		return nil, 0
	}

	// Return an error if the index doesn't exist.
	if index > (uint64(len(l.entries)) + l.startIndex) {
		panic(fmt.Sprintf("raft: Index is beyond end of log: %v %v", len(l.entries), index))
	}

	// If we're going from the beginning of the log then return the whole log.
	if index == l.startIndex {
		traceln("log.entriesAfter.beginning: ", index, " ", l.startIndex)
		return l.entries, l.startTerm
	}

	traceln("log.entriesAfter.partial: ", index, " ", l.entries[len(l.entries)-1].Index())

	entries := l.entries[index-l.startIndex:]
	length := len(entries)

	traceln("log.entriesAfter: startIndex:", l.startIndex, " length", len(l.entries))

	if uint64(length) < maxLogEntriesPerRequest {
		// Determine the term at the given entry and return a subslice.
		return entries, l.entries[index-1-l.startIndex].Term()
	} else {
		return entries[:maxLogEntriesPerRequest], l.entries[index-1-l.startIndex].Term()
	}
}


// The last committed index in the log.
func (l *Log) CommitIndex() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.commitIndex
}


// Truncates the log to the given index and term. This only works if the log
// at the index has not been committed.
func (l *Log) truncate(index uint64, term uint64) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	debugln("log.truncate: ", index)

	// Do not allow committed entries to be truncated.
	if index < l.commitIndex {
		debugln("log.truncate.before")
		return fmt.Errorf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", l.commitIndex, index, term)
	}

	// Do not truncate past end of entries.
	if index > l.startIndex+uint64(len(l.entries)) {
		debugln("log.truncate.after")
		return fmt.Errorf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)
	}

	// If we're truncating everything then just clear the entries.
	if index == l.startIndex {
		debugln("log.truncate.clear")
		l.file.Truncate(0)
		l.file.Seek(0, os.SEEK_SET)

		// notify clients if this node is the previous leader
		for _, entry := range l.entries {
			if entry.event != nil {
				entry.event.c <- errors.New("command failed to be committed due to node failure")
			}
		}

		l.entries = []*LogEntry{}
	} else {
		// Do not truncate if the entry at index does not have the matching term.
		entry := l.entries[index-l.startIndex-1]
		if len(l.entries) > 0 && entry.Term() != term {
			debugln("log.truncate.termMismatch")
			return fmt.Errorf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term(), index, term)
		}

		// Otherwise truncate up to the desired entry.
		if index < l.startIndex+uint64(len(l.entries)) {
			debugln("log.truncate.finish")
			position := l.entries[index-l.startIndex].Position
			l.file.Truncate(position)
			l.file.Seek(position, os.SEEK_SET)

			// notify clients if this node is the previous leader
			for i := index - l.startIndex; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				if entry.event != nil {
					entry.event.c <- errors.New("command failed to be committed due to node failure")
				}
			}

			l.entries = l.entries[0 : index-l.startIndex]
		}
	}

	return nil
}


// Appends a series of entries to the log.
func (l *Log) appendEntries(entries []*raftpb.LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	startPosition, _ := l.file.Seek(0, os.SEEK_CUR)

	w := bufio.NewWriter(l.file)

	var size int64
	var err error
	// Append each entry but exit if we hit an error.
	for i := range entries {
		logEntry := &LogEntry{
			log:      l,
			Position: startPosition,
			pb:       entries[i],
		}

		if size, err = l.writeEntry(logEntry, w); err != nil {
			return err
		}

		startPosition += size
	}
	w.Flush()
	err = l.sync()

	if err != nil {
		panic(err)
	}

	return nil
}

// appendEntry with Buffered io
func (l *Log) writeEntry(entry *LogEntry, w io.Writer) (int64, error) {
	if l.file == nil {
		return -1, errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term() < lastEntry.Term() {
			return -1, fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		} else if entry.Term() == lastEntry.Term() && entry.Index() <= lastEntry.Index() {
			return -1, fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}
	}

	// Write to storage.
	size, err := entry.WriteTo(w)
	if err != nil {
		return -1, err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return int64(size), nil
}

func (l *Log) sync() error {
	return l.file.Sync()
}


// Retrieves the last index and term that has been appended to the log.
func (l *Log) lastInfo() (index uint64, term uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// If we don't have any entries then just return zeros.
	if len(l.entries) == 0 {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term
	entry := l.entries[len(l.entries)-1]
	return entry.Index(), entry.Term()
}

type Command interface {
	CommandName() *string
}

// Creates a new log entry associated with a log.
func newLogEntry(log *Log, event *ev, index uint64, term uint64, command Command) (*LogEntry, error) {
	var buf bytes.Buffer
	json.NewEncoder(&buf).Encode(command)
	name := ""
	nameAddr := &name
	if (command != nil) {
		nameAddr = command.CommandName()
	}
	pb := &raftpb.LogEntry{
		Index:       &index,
		Term:        &term,
		CommandName:nameAddr,
		Command:     buf.Bytes(),
	}

	e := &LogEntry{
		pb:    pb,
		log:   log,
		event: event,
	}

	return e, nil
}