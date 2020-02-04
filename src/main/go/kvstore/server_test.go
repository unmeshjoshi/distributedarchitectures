package kvstore

import "testing"

func TestServer(t *testing.T) {
	server, err := NewServer()
	if (err == nil) {
		server.run()
	}
}