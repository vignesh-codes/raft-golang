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
	node.State = Candidate
	node.CurrentTerm++
	node.VotedFor = node.ID
	votesReceived := 1 // Vote for self.
	node.mu.Unlock()

	log.Printf("Node %s started election for term %d", node.ID, node.CurrentTerm)

	var wg sync.WaitGroup
	for _, peer := range node.Peers {
		wg.Add(1)
		go func(peer PeerNode) {
			defer wg.Done()
			requestVote(peer, &votesReceived)
		}(peer)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("Election timed out for term %d", node.CurrentTerm)
	}

	node.mu.Lock()
	defer node.mu.Unlock()
	if votesReceived > len(node.Peers)/2 {
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		log.Printf("Node %s is elected as leader for term %d", node.ID, node.CurrentTerm)
		go sendHeartbeats()
	} else {
		log.Printf("Election failed for term %d with %d votes, retrying...", node.CurrentTerm, votesReceived)
		node.mu.Unlock()
		time.Sleep(time.Duration(rand.Intn(150)+150) * time.Millisecond)
		startElection()
		return
	}
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
		// Reject the vote if the candidate's term is outdated.
		log.Printf("Rejecting vote request from candidate %s: outdated term %d", req.CandidateID, req.Term)
	} else if req.Term > node.CurrentTerm {
		// Update term and step down to follower if the candidate's term is newer.
		node.CurrentTerm = req.Term
		node.VotedFor = ""
		node.State = Follower
	}

	// Grant vote only if the candidate's log is up-to-date
	if req.Term == node.CurrentTerm && (req.LastLogIndex >= len(node.Log) || node.Log[req.LastLogIndex].Term == req.Term) {
		node.VotedFor = req.CandidateID
		voteGranted = true
		electionResetChan <- true
		log.Printf("Vote granted to candidate %s", req.CandidateID)
	}

	resp := VoteResponse{Term: node.CurrentTerm, VoteGranted: voteGranted}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func requestVote(peer PeerNode, votesReceived *int) {
	req := VoteRequest{Term: node.CurrentTerm, CandidateID: node.ID, LastLogIndex: len(node.Log)}
	data, _ := json.Marshal(req)
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		if peer.ID == node.ID {
			*votesReceived++
			break
		}
		resp, err := http.Post(peer.Address+"/vote", "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Failed to request vote from %s (attempt %d): %v", peer.Address, i+1, err)
			time.Sleep(time.Duration(rand.Intn(100)+100) * time.Millisecond)
			if i <= maxRetries {
				// remove this peer from peers
				node.mu.Lock()
				delete(node.Peers, peer.ID)
				fmt.Println("Updated peers list:", node.Peers)
				node.mu.Unlock()
			}
			continue
		}
		var voteResp VoteResponse
		if err := json.NewDecoder(resp.Body).Decode(&voteResp); err == nil && voteResp.VoteGranted {
			*votesReceived++
			log.Printf("Vote granted by %s", peer.Address)
		} else {
			log.Printf("Failed to get vote from %s: Term %d, Granted: %v", peer.Address, voteResp.Term, voteResp.VoteGranted)
		}
		resp.Body.Close()
		break
	}

	if *votesReceived == 0 {
		log.Printf("Node %s failed to get a vote from %s after %d retries", node.ID, peer.Address, maxRetries)
	}
}
