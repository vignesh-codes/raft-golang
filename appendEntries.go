package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// AppendEntriesRequest is used to replicate log entries (and as heartbeat).
type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
	Address      string     `json:"address"`
}

// AppendEntriesResponse is returned by followers after processing AppendEntries.
type AppendEntriesResponse struct {
	Term         int  `json:"term"`
	Success      bool `json:"success"`
	PrevLogIndex int  `json:"prev_log_index"`
	LastLogIndex int  `json:"lastLogIndex"` // NEW: Last log index of follower
	CommitIndex  int  `json:"commitIndex"`  // NEW: Commit index of follower
}

// handleAppendEntries processes the AppendEntries RPC from the leader.
func handleAppendEntries(w http.ResponseWriter, r *http.Request) {

	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Println("Error decoding request:", err)
		http.Error(w, "Invalid append entries request", http.StatusBadRequest)
		return
	}
	if node.State == Leader {
		stopHeartbeats()
		fmt.Println("another one is also a leader which is bad")
		node.State = Follower
		startLeaderCheck()
		node.Leader = PeerNode{ID: req.LeaderID, Address: req.Address}
	}
	node.Leader = PeerNode{ID: req.LeaderID, Address: req.Address}
	node.mu.Lock()
	defer node.mu.Unlock()

	fmt.Printf("AppendEntries request: Term=%d, LeaderID=%s, PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, LeaderCommit=%d\n",
		req.Term, req.LeaderID, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)

	// Reject outdated term
	if req.Term < node.CurrentTerm {
		fmt.Println("Rejecting: Leader's term is outdated.")
		resp := AppendEntriesResponse{Term: node.CurrentTerm, Success: false, LastLogIndex: len(node.Log) - 1, CommitIndex: node.CommitIndex}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Reset election timeout
	electionResetChan <- true

	// If prevLogIndex > 0, ensure consistency
	if req.PrevLogIndex >= 1 {
		if len(node.Log) <= req.PrevLogIndex || node.Log[req.PrevLogIndex].Term != req.PrevLogTerm {
			fmt.Println("Rejecting: Log mismatch at PrevLogIndex.")
			resp := AppendEntriesResponse{
				Term:         node.CurrentTerm,
				Success:      false,
				LastLogIndex: len(node.Log) - 1, // Send correct log index
				CommitIndex:  node.CommitIndex,  // Send commit index
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
	}

	// Append new entries
	newIndex := req.PrevLogIndex + 1
	for i, entry := range req.Entries {
		if len(node.Log) > newIndex {
			if node.Log[newIndex].Term != entry.Term {
				node.Log = node.Log[:newIndex] // Truncate conflicting entries
				node.Log = append(node.Log, req.Entries[i:]...)
				break
			}
		} else {
			node.Log = append(node.Log, entry)
		}
		newIndex++
	}

	// Update commit index
	if req.LeaderCommit > node.CommitIndex {
		node.CommitIndex = min(req.LeaderCommit, len(node.Log)-1)
	}

	fmt.Printf("Fllower logs: %+v\n", node.Log)
	resp := AppendEntriesResponse{
		Term:         node.CurrentTerm,
		Success:      true,
		LastLogIndex: len(node.Log) - 1, // Send last index to leader
		CommitIndex:  node.CommitIndex,  // Send commit index
	}
	json.NewEncoder(w).Encode(resp)
}

// sendAppendEntries sends an AppendEntries RPC to a peer.
// retry three times if the request fails.
func sendAppendEntries(peer PeerNode, req AppendEntriesRequest) (*AppendEntriesResponse, error) {
	var response = &AppendEntriesResponse{}
	for i := 0; i < 3; i++ {
		fmt.Println("sending append entries date", req)
		data, _ := json.Marshal(req)
		resp, err := http.Post(peer.Address+"/appendEntries", "application/json", bytes.NewBuffer(data))
		if err != nil {
			// Retry on error after 2 seconds.
			time.Sleep(2 * time.Second)
			continue
		}
		defer resp.Body.Close()
		var response AppendEntriesResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		return &response, nil
	}
	return response, nil
}
