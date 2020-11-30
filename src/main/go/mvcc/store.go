package mvcc

import bolt "go.etcd.io/bbolt"

type store struct {
	kvindex index
	db * bolt.DB
}

func (s *store) put() {
	db.Begin()
}