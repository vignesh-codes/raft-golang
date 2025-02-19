package main

import (
	"math/rand"
	"sync"
	"time"
)

// --------------------- Data Structures ---------------------

type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
	Leader    NodeState = "Leader"
)

type PeerNode struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type LogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command string `json:"command"`
}

type RaftNode struct {
	ID          string
	Address     string
	Peers       map[string]PeerNode
	Leader      PeerNode
	CurrentTerm int
	VotedFor    string
	State       NodeState
	Log         []LogEntry
	CommitIndex int
	NextIndex   map[string]int
	mu          sync.Mutex
}

var node RaftNode
var electionResetChan = make(chan bool)

// random election timeout between 8 and 18 seconds
var electionTimeout = time.Duration(rand.Intn(10)+8) * time.Second
var unResponsivePeers = make(map[string]string)

// --------------------- Vote RPC Structures ---------------------

type VoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// --------------------- AppendEntries RPC Structures ---------------------

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
	Address      string     `json:"address"`
}

type AppendEntriesResponse struct {
	Term         int  `json:"term"`
	Success      bool `json:"success"`
	PrevLogIndex int  `json:"prev_log_index"`
	LastLogIndex int  `json:"lastLogIndex"` // Last log index of follower
	CommitIndex  int  `json:"commitIndex"`  // Commit index of follower
}

type JoinResponse struct {
	Leader      PeerNode            `json:"leader"`
	Peers       map[string]PeerNode `json:"peers"`
	CurrentTerm int                 `json:"current_term"`
}
