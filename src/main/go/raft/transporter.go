package raft

type Transporter interface {
	//SendVoteRequest(server server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse
	SendAppendEntriesRequest(server *server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse

}
