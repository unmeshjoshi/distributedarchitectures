package kvstore

import (
	"consensus/newraft/raftpb"
	pb "consensus/newraft/raftpb"
	"consensus/newraft/wal"
	"fmt"
)

type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(pb.Snapshot) error
	// Close closes the Storage and performs finalization.
	Close() error
}

func NewStorage(w *wal.WAL, s *Snapshotter) Storage {
	return &storage{w, s}
}

type storage struct {
	*wal.WAL
	*Snapshotter
}

func (st *storage) SaveSnap(pb.Snapshot) error {
	fmt.Printf("Savesnap called on (%v)", st)
	return nil
}


type Snapshotter struct {

}
