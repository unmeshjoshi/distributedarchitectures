package raft

import "fmt"

type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st string, ents []uint) error
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap string) error
	// Close closes the Storage and performs finalization.
	Close() error
}

func NewStorage(w *WAL, s *Snapshotter) Storage {
	return &storage{w, s}
}

type storage struct {
	*WAL
	*Snapshotter
}

func (st *storage) SaveSnap(snap string) error {
	fmt.Printf("Savesnap called on (%v)", st)
	return nil
}

type WAL struct {
}

func (wl *WAL) 	Close() error {
	fmt.Printf("Close called on (%v)", wl)
	return nil
}

func (wl *WAL) Save(st string, ents []uint) error {
	fmt.Printf("Save called on (%v)", wl)
	return nil
}

type Snapshotter struct {

}
