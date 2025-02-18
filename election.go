package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// VoteRequest and VoteResponse for leader elections.
type VoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// startElection initiates the leader election process.
func startElection() {
	node.mu.Lock()

	// Move to candidate state
	node.State = Candidate
	node.CurrentTerm++
	node.VotedFor = node.ID
	votesReceived := 1
	currentTerm := node.CurrentTerm

	node.mu.Unlock()

	log.Printf("Node %s started election for term %d", node.ID, currentTerm)
	var wg sync.WaitGroup
	voteResponses := make(chan int, len(node.Peers))
	fmt.Println("requesting votes from these peers ", node.Peers)
	for _, peer := range node.Peers {
		if peer.ID == node.ID {
			continue
		}
		wg.Add(1)
		go func(peer PeerNode) {
			defer wg.Done()
			voteGranted := requestVote(peer, currentTerm)
			voteResponses <- voteGranted
		}(peer)
	}

	// Close channel after collecting votes
	go func() {
		wg.Wait()
		close(voteResponses)
	}()

	// Count votes
	votesReceived += <-voteResponses

	// Lock before updating node state
	// node.mu.Lock()
	// defer node.mu.Unlock()

	if node.CurrentTerm != currentTerm {
		log.Printf("Election for term %d is outdated", currentTerm)
		return
	}
	fmt.Println("required votes:", len(node.Peers)/2)
	fmt.Println("votes received:", votesReceived)
	if len(node.Peers) == 2 {
		// sleep for random seconds between 0 and 15 seconds
		time.Sleep(time.Duration(rand.Intn(15)) * time.Second)
	}
	// Majority vote check
	if votesReceived >= len(node.Peers)/2 {
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		log.Printf("Node %s is elected as leader for term %d", node.ID, node.CurrentTerm)
		startHeartbeats()
		stopLeaderCheck()
		// go sendHeartbeats()
	} else {
		log.Printf("Election failed for term %d with %d votes", node.CurrentTerm, votesReceived)
		// electionResetChan <- true
	}
}

// -------------------- Request Vote from Peers --------------------

func requestVote(peer PeerNode, term int) int {
	client := &http.Client{Timeout: 2 * time.Second} // Set request timeout
	req := VoteRequest{Term: term, CandidateID: node.ID, LastLogIndex: len(node.Log)}
	data, _ := json.Marshal(req)

	resp, err := client.Post(peer.Address+"/vote", "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Failed to request vote from %s: %v", peer.Address, err)
		// trying to acquaire lock
		fmt.Println("[[[ trying to acquire node lock for removing unresponsive peers ]]] \n")
		node.mu.Lock()
		// remove peers if exists
		node.Peers = removePeer(node.Peers, peer)
		fmt.Println("removed unresponsive peers ", peer)
		node.mu.Unlock()
		return 0
	}
	defer resp.Body.Close()

	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		log.Printf("Invalid response from %s", peer.Address)
		return 0
	}

	// Update term if outdated
	node.mu.Lock()
	defer node.mu.Unlock()
	if voteResp.Term > node.CurrentTerm {
		node.CurrentTerm = voteResp.Term
		node.State = Follower
		startLeaderCheck()
		node.VotedFor = ""
		return 0
	}

	return 1
}

func removePeer(peers map[string]PeerNode, peer PeerNode) map[string]PeerNode {
	// Remove the peer from the map if exists
	_, ok := peers[peer.ID]
	if !ok {
		fmt.Println("peer not found")
		return peers
	}
	delete(peers, peer.ID)
	return peers
}

func handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	voteGranted := false
	if req.Term < node.CurrentTerm {
		log.Printf("Rejecting vote request from %s: outdated term %d", req.CandidateID, req.Term)
	} else if req.Term > node.CurrentTerm {
		node.CurrentTerm = req.Term
		node.VotedFor = ""
		node.State = Follower
		startLeaderCheck()
	}

	if req.Term == node.CurrentTerm && (req.LastLogIndex >= len(node.Log) || len(node.Log) == 0) {
		node.VotedFor = req.CandidateID
		voteGranted = true
	}

	resp := VoteResponse{Term: node.CurrentTerm, VoteGranted: voteGranted}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
