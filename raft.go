package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
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

// PeerNode represents a node in the cluster
type PeerNode struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// RaftNode holds all state information
type RaftNode struct {
	ID          string              `json:"id"`
	Address     string              `json:"address"`
	Peers       map[string]PeerNode `json:"peers"`
	Leader      PeerNode            `json:"leader"`
	CurrentTerm int                 `json:"current_term"`
	VotedFor    string              `json:"voted_for"`
	State       NodeState           `json:"state"`
	mu          sync.Mutex
}

// Vote request and response
type VoteRequest struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// JoinResponse structure
type JoinResponse struct {
	Leader PeerNode            `json:"leader"`
	Peers  map[string]PeerNode `json:"peers"`
}

var node RaftNode
var electionTimeout time.Duration
var electionResetChan = make(chan bool)

// Handle node joining
func handleJoin(w http.ResponseWriter, r *http.Request) {
	var newNode PeerNode
	err := json.NewDecoder(r.Body).Decode(&newNode)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	node.Peers[newNode.ID] = newNode
	log.Printf("New node joined: %s", newNode.Address)
	node.mu.Unlock()

	// Send leader and peer information
	response := JoinResponse{Leader: node.Leader, Peers: node.Peers}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	// If leader, sync peer updates
	if node.State == Leader {
		go syncPeers()
	}
}

func syncPeers() {
	node.mu.Lock()
	defer node.mu.Unlock()
	fmt.Println("syncing peers")
	// Prepare the updated peer list
	peerUpdate := JoinResponse{Leader: node.Leader, Peers: node.Peers}
	peerUpdateJSON, _ := json.Marshal(peerUpdate)

	// Send update to all peers
	for _, peer := range node.Peers {
		if peer.ID == node.ID {
			continue // Skip self
		}
		go func(peer PeerNode) {
			_, err := http.Post(peer.Address+"/sync", "application/json", bytes.NewBuffer(peerUpdateJSON))
			if err != nil {
				log.Printf("Failed to sync with peer %s: %v", peer.Address, err)
			}
		}(peer)
	}
}

func handleSync(w http.ResponseWriter, r *http.Request) {
	var updatedPeers JoinResponse
	err := json.NewDecoder(r.Body).Decode(&updatedPeers)
	if err != nil {
		http.Error(w, "Invalid sync request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	node.Peers = updatedPeers.Peers
	node.Leader = updatedPeers.Leader
	node.mu.Unlock()

	log.Printf("Synced with leader: %s and %d peers", node.Leader.Address, len(node.Peers))
}

// Handle vote requests
func handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var voteReq VoteRequest
	err := json.NewDecoder(r.Body).Decode(&voteReq)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	voteGranted := false
	if voteReq.Term > node.CurrentTerm {
		node.CurrentTerm = voteReq.Term
		node.VotedFor = voteReq.CandidateID
		voteGranted = true
		electionResetChan <- true
	}

	resp := VoteResponse{Term: node.CurrentTerm, VoteGranted: voteGranted}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Handle leader heartbeats
func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	electionResetChan <- true
	w.WriteHeader(http.StatusOK)
}

// Start Election Process
// Start Election Process with timeout and retries
func startElection() {
	node.mu.Lock()
	node.State = Candidate
	node.CurrentTerm++
	node.VotedFor = node.ID
	votesReceived := 1 // Vote for self
	node.mu.Unlock()

	log.Printf("Node %s started election for term %d", node.ID, node.CurrentTerm)
	log.Printf("with peers ", node.Peers)

	// Request votes from peers with a timeout
	voteTimeout := 5 * time.Second
	votesChan := make(chan bool, len(node.Peers))

	// Request votes asynchronously
	for _, peer := range node.Peers {
		go func(peer PeerNode) {
			requestVote(peer, &votesReceived)
			votesChan <- true
		}(peer)
	}

	// Wait for votes or timeout
	select {
	case <-time.After(voteTimeout):
		log.Printf("Election timed out for term %d", node.CurrentTerm)
	case <-votesChan:
		// Continue if votes received
	}

	// Election result check after vote timeout or successful votes
	node.mu.Lock()
	defer node.mu.Unlock()

	if votesReceived > len(node.Peers)/2 {
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		log.Printf("Node %s is the new leader!", node.ID)
		go sendHeartbeats()
	} else {
		log.Println("Election failed or partial votes. Retrying...")
		time.Sleep(time.Duration(rand.Intn(150)+150) * time.Millisecond)
		startElection() // Retry election
	}
}

// Request vote from peer with retries and timeout handling
func requestVote(peer PeerNode, votesReceived *int) {
	voteReq := VoteRequest{Term: node.CurrentTerm, CandidateID: node.ID}
	voteReqJSON, _ := json.Marshal(voteReq)

	// Define a retry strategy for unreachable peers
	const maxRetries = 3
	const timeout = 2 * time.Second

	for retries := 0; retries < maxRetries; retries++ {
		// consider voting for self
		if peer.ID == node.ID {
			*votesReceived++
			break // Skip network request for self
		}
		resp, err := http.Post(peer.Address+"/vote", "application/json", bytes.NewBuffer(voteReqJSON))
		if err != nil {
			log.Printf("Failed to request vote from peer %s (attempt %d/%d): %v", peer.Address, retries+1, maxRetries, err)
			time.Sleep(time.Duration(rand.Intn(100)+100) * time.Millisecond) // Small delay between retries
			continue                                                         // Retry if there was a network error or timeout
		}
		defer resp.Body.Close()

		// Parse the response
		var voteResp VoteResponse
		if err := json.NewDecoder(resp.Body).Decode(&voteResp); err == nil && voteResp.VoteGranted {
			// Vote granted successfully
			*votesReceived++
			log.Printf("Vote granted by %s", peer.Address)
			break
		} else {
			log.Printf("Failed to get vote from %s: Term %d, Granted: %v", peer.Address, voteResp.Term, voteResp.VoteGranted)
			break
		}
	}

	// Log if all retries fail for this peer
	if *votesReceived == 0 {
		log.Printf("Node %s failed to get a vote from peer %s after %d retries", node.ID, peer.Address, maxRetries)
	}
}

// Send heartbeats to followers
func sendHeartbeats() {
	for node.State == Leader {
		time.Sleep(5 * time.Second)
		log.Println("Leader sending heartbeats...")
		// We need to iterate over a copy of the peers to avoid issues
		node.mu.Lock()
		peersCopy := make([]PeerNode, 0, len(node.Peers))
		for _, peer := range node.Peers {
			peersCopy = append(peersCopy, peer)
		}
		node.mu.Unlock()

		for _, peer := range peersCopy {
			resp, err := http.Post(peer.Address+"/heartbeat", "application/json", nil)
			if err != nil {
				log.Printf("Failed to send heartbeat to peer %s: %v", peer.Address, err)
				// Remove this peer from the peers list
				node.mu.Lock()
				delete(node.Peers, peer.ID)
				fmt.Println("Updated peers are:", node.Peers)
				node.mu.Unlock()
				// Sync the updated peers with others
				syncPeers()
				// Skip calling resp.Body.Close() since resp is nil
				continue
			}
			// Only close resp.Body if resp is not nil
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

// Detect leader failure and start election
func startLeaderCheck() {
	for {
		select {
		case <-electionResetChan:
			continue // Reset election timer
		case <-time.After(electionTimeout):
			log.Println("Leader timeout! Starting election...")
			startElection()
		}
	}
}

// Start HTTP server
func startServer(port string) {
	http.HandleFunc("/join", handleJoin)
	http.HandleFunc("/vote", handleVoteRequest)
	http.HandleFunc("/heartbeat", handleHeartbeat)
	http.HandleFunc("/sync", handleSync)
	log.Printf("Node %s listening on port %s...", node.ID, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Register the node with a known peer
func joinCluster(peerAddress string) {
	newNode := PeerNode{ID: node.ID, Address: node.Address}
	newNodeJSON, _ := json.Marshal(newNode)

	resp, err := http.Post(peerAddress+"/join", "application/json", bytes.NewBuffer(newNodeJSON))
	if err != nil {
		log.Fatalf("Failed to join cluster at %s: %v", peerAddress, err)
	}
	defer resp.Body.Close()

	var joinResponse JoinResponse
	err = json.NewDecoder(resp.Body).Decode(&joinResponse)
	if err != nil {
		log.Fatalf("Failed to parse join response: %v", err)
	}

	node.mu.Lock()
	node.Leader = joinResponse.Leader
	node.Peers = joinResponse.Peers
	node.mu.Unlock()

	log.Printf("Joined cluster. Leader: %s, Peers: %d", node.Leader.Address, len(node.Peers))
}

func main() {
	fmt.Println("args are ", os.Args)
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run raft_node.go <port> <optional: known peer>")
		return
	}
	fmt.Println("args are ", len(os.Args))
	port := os.Args[1]
	knownPeer := ""
	if len(os.Args) == 3 {
		knownPeer = os.Args[2]
	}

	rand.Seed(time.Now().UnixNano())
	// election timeout = 10s
	// random between 10-15 seconds
	electionTimeout = time.Duration(rand.Intn(5)+10) * time.Second

	node = RaftNode{
		ID:      "node-" + port,
		Address: "http://localhost:" + port,
		Peers:   make(map[string]PeerNode),
		State:   Follower,
	}
	fmt.Println("known peers are ", knownPeer)

	if knownPeer == "" {
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		go sendHeartbeats()
	} else {
		joinCluster(knownPeer)
		go startLeaderCheck()
	}

	startServer(port)
}
