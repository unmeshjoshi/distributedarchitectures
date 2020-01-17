package raft

import "testing"

func TestStorage(t *testing.T) {
	var newStorage Storage = NewStorage(&WAL{}, &Snapshotter{})
	uints := []uint{2, 3, 5, 7, 11, 13}
	newStorage.Save("test", uints)
	newStorage.SaveSnap("snap")
	newStorage.Close()
}