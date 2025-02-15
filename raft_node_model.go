package main

import "sync"

// NodeState defines the role of a node in the cluster
type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
	Leader    NodeState = "Leader"
)

// PeerNode represents a node in the cluster
type PeerNode struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// JoinResponse contains leader info and peer list
type JoinResponse struct {
	Leader PeerNode            `json:"leader"`
	Peers  map[string]PeerNode `json:"peers"`
}

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	ID          string              `json:"id"`
	Address     string              `json:"address"`
	Peers       map[string]PeerNode `json:"peers"`
	Leader      PeerNode            `json:"leader"`
	CurrentTerm int                 `json:"current_term"`
	VotedFor    string              `json:"voted_for"`
	State       NodeState           `json:"state"`
	mu          sync.Mutex          // Synchronization lock
}
