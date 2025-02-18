package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Node states
type NodeState string

const (
	Follower  NodeState = "Follower"
	Candidate NodeState = "Candidate"
	Leader    NodeState = "Leader"
	Inactive  NodeState = "Inactive"
)

// PeerNode represents a node in the cluster.
type PeerNode struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// LogEntry represents a single entry in the replicated log.
type LogEntry struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command string `json:"command"`
}

// RaftNode holds all state information.
type RaftNode struct {
	ID          string              `json:"id"`
	Address     string              `json:"address"`
	Peers       map[string]PeerNode `json:"peers"`
	Leader      PeerNode            `json:"leader"`
	CurrentTerm int                 `json:"current_term"`
	VotedFor    string              `json:"voted_for"`
	State       NodeState           `json:"state"`
	Log         []LogEntry          `json:"log"`
	CommitIndex int                 `json:"commit_index"`
	NextIndex   map[string]int      `json:"next_index"`
	mu          sync.Mutex
}

// JoinResponse structure is used when a node joins the cluster.
type JoinResponse struct {
	Leader      PeerNode            `json:"leader"`
	Peers       map[string]PeerNode `json:"peers"`
	CurrentTerm int                 `json:"current_term"`
}

var node RaftNode
var electionTimeout time.Duration
var electionResetChan = make(chan bool)

// handleJoin processes a join request from a new node.
func handleJoin(w http.ResponseWriter, r *http.Request) {
	var newNode PeerNode
	if err := json.NewDecoder(r.Body).Decode(&newNode); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	node.Peers[newNode.ID] = newNode
	log.Printf("New node joined: %s", newNode.Address)
	node.mu.Unlock()

	response := JoinResponse{Leader: node.Leader, Peers: node.Peers, CurrentTerm: node.CurrentTerm}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	if node.State == Leader {
		go syncPeers()
	}
}

// startServer starts the HTTP server and registers RPC endpoints.
func startServer(port string) {
	http.HandleFunc("/join", handleJoin)
	http.HandleFunc("/sync", handleSync)
	http.HandleFunc("/vote", handleVoteRequest)
	http.HandleFunc("/heartbeat", handleHeartbeat)
	http.HandleFunc("/appendEntries", handleAppendEntries)
	http.HandleFunc("/getLog", handleGetLog)
	http.HandleFunc("/newCommand", handleNewCommandEntry)
	log.Printf("Node %s listening on port %s...", node.ID, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleGetLog(w http.ResponseWriter, r *http.Request) {
	node.mu.Lock()
	defer node.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(node.Log)
}

// joinCluster allows this node to join an existing cluster.
func joinCluster(peerAddress string) {
	if !strings.HasPrefix(peerAddress, "http://") {
		peerAddress = "http://" + peerAddress
	}
	newNode := PeerNode{ID: node.ID, Address: node.Address}
	data, _ := json.Marshal(newNode)
	resp, err := http.Post(peerAddress+"/join", "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Fatalf("Failed to join cluster at %s: %v", peerAddress, err)
	}
	defer resp.Body.Close()
	var joinResp JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&joinResp); err != nil {
		log.Fatalf("Failed to parse join response: %v", err)
	}
	node.mu.Lock()
	node.Leader = joinResp.Leader
	node.Peers = joinResp.Peers
	node.CurrentTerm = joinResp.CurrentTerm
	node.mu.Unlock()
	log.Printf("Joined cluster. Leader: %s, Total peers: %d", node.Leader.Address, len(node.Peers))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run raft_node.go <port> <optional: known peer>")
		return
	}
	port := os.Args[1]
	knownPeer := ""
	if len(os.Args) == 3 {
		knownPeer = os.Args[2]
	}
	rand.Seed(time.Now().UnixNano())
	electionTimeout = time.Duration(rand.Intn(5)+10) * time.Second

	node = RaftNode{
		ID:        "node-" + port,
		Address:   "http://localhost:" + port,
		Peers:     make(map[string]PeerNode),
		State:     Follower,
		Log:       make([]LogEntry, 0),
		NextIndex: make(map[string]int),
	}
	fmt.Println("Known peer (if any):", knownPeer)

	if knownPeer == "" {
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		node.Peers[node.ID] = PeerNode{ID: node.ID, Address: node.Address}
		go sendHeartbeats()
	} else {
		joinCluster(knownPeer)
		go startLeaderCheck()
	}
	startServer(port)
}
