package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

func stepUpToLeader() {
	node.mu.Lock()
	node.State = Leader
	node.Leader = PeerNode{ID: node.ID, Address: node.Address}
	stopLeaderCheck()
	startHeartbeats()
	node.mu.Unlock()
}

func stepDownFromCandidate() {
	node.mu.Lock()
	node.State = Follower
	startLeaderCheck()
	node.mu.Unlock()
}

func stepDownFromLeader(leaderID, addr string) {
	stopHeartbeats()
	node.mu.Lock()
	node.State = Follower
	startLeaderCheck()
	node.Leader = PeerNode{ID: leaderID, Address: addr}
	node.mu.Unlock()
}

// --------------------- Vote RPC Handler ---------------------

func handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()

	var voteGranted bool
	// Reject if candidate's term is outdated.
	if req.Term < node.CurrentTerm {
		log.Printf("Rejecting vote request from %s: outdated term %d", req.CandidateID, req.Term)
		voteGranted = false
	} else if req.Term > node.CurrentTerm {
		// Update term and become follower.
		node.CurrentTerm = req.Term
		node.VotedFor = ""
		node.State = Follower
	}
	// If already leader, do not grant vote.
	if node.State == Leader {
		voteGranted = false
	} else if node.VotedFor == "" || node.VotedFor == req.CandidateID {
		// (Assume candidate's log is at least as up-to-date.)
		voteGranted = true
		node.VotedFor = req.CandidateID
		// Reset election timer.
		electionResetChan <- true
	}

	resp := VoteResponse{Term: node.CurrentTerm, VoteGranted: voteGranted}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --------------------- Vote Request (RPC Client) ---------------------

// requestVote sends a vote request to a peer; returns 1 if granted, 0 otherwise.
func requestVote(peer PeerNode, term int) int {
	client := &http.Client{Timeout: 2 * time.Second}
	node.mu.Lock()
	lastLogIndex := len(node.Log)
	node.mu.Unlock()

	reqPayload := VoteRequest{
		Term:         term,
		CandidateID:  node.ID,
		LastLogIndex: lastLogIndex,
	}
	data, _ := json.Marshal(reqPayload)
	resp, err := client.Post(peer.Address+"/vote", "application/json", bytes.NewBuffer(data))
	if err != nil {
		// log.Printf("Failed to request vote from %s: %v", peer.Address, err)
		unResponsivePeers[peer.ID] = peer.Address
		return 0
	}
	defer resp.Body.Close()
	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		log.Printf("Invalid response from %s", peer.Address)
		return 0
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	if voteResp.Term > node.CurrentTerm {
		node.CurrentTerm = voteResp.Term
		node.State = Follower
		node.VotedFor = ""
		return 0
	}
	if voteResp.VoteGranted {
		return 1
	}
	return 0
}

// --------------------- Leader Election ---------------------

func startElection() {
	node.mu.Lock()
	node.State = Candidate
	node.CurrentTerm++
	node.VotedFor = node.ID
	votesReceived := 1 // Vote for self.
	currentTerm := node.CurrentTerm
	node.mu.Unlock()

	log.Printf("Node %s started election for term %d", node.ID, currentTerm)

	var wg sync.WaitGroup
	voteResponses := make(chan int, len(node.Peers))
	// Request votes from all peers.
	for _, peer := range node.Peers {
		if peer.ID == node.ID {
			continue
		}
		wg.Add(1)
		go func(peer PeerNode) {
			defer wg.Done()
			vote := requestVote(peer, currentTerm)
			voteResponses <- vote
		}(peer)
	}
	// Close the channel after all vote requests finish.
	go func() {
		wg.Wait()
		close(voteResponses)
	}()
	// Sum up the votes.
	for v := range voteResponses {
		votesReceived += v
	}
	node.mu.Lock()

	if node.CurrentTerm != currentTerm {
		log.Printf("Election for term %d is outdated. Current term: %d", currentTerm, node.CurrentTerm)
		return
	}
	requiredVotes := (len(node.Peers) + 1) / 2
	// If a peer is unresponsive, remove it from the peers list.
	toSync := false
	for id, addr := range unResponsivePeers {
		delete(node.Peers, id)
		toSync = true
		log.Printf("Removed unresponsive peer %s: %s", id, addr)
		fmt.Println("current peers ", node.Peers)
		requiredVotes -= 1
	}
	if toSync {
		syncPeers()
	}
	// Only become leader if a strict majority is achieved.
	if votesReceived > requiredVotes {
		node.mu.Unlock()
		log.Printf("Node %s is elected leader for term %d with %d votes", node.ID, node.CurrentTerm, votesReceived)
		stepUpToLeader()
		return
	} else {
		node.mu.Unlock()
		log.Printf("Election failed for term %d with %d votes", node.CurrentTerm, votesReceived)
		stepDownFromCandidate()
		return
	}
}
