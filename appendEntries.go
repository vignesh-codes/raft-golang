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
}

// AppendEntriesResponse is returned by followers after processing AppendEntries.
type AppendEntriesResponse struct {
	Term         int  `json:"term"`
	Success      bool `json:"success"`
	PrevLogIndex int  `json:"prev_log_index"`
}

// handleAppendEntries processes the AppendEntries RPC from the leader.
func handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received AppendEntries request")

	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Println("Error decoding request:", err)
		http.Error(w, "Invalid append entries request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	fmt.Printf("AppendEntries request details: Term=%d, LeaderID=%d, PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, LeaderCommit=%d\n",
		req.Term, req.LeaderID, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)

	// If the leader's term is less than our term, reject the request.
	if req.Term < node.CurrentTerm {
		fmt.Println("Rejecting request: Leader's term is outdated.")
		resp := AppendEntriesResponse{Term: node.CurrentTerm, Success: false}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Reset the election timer.
	fmt.Println("Resetting election timer")
	electionResetChan <- true

	// If prevLogIndex > 0, ensure our log has an entry at that index with a matching term.
	if req.PrevLogIndex > 0 {
		fmt.Printf("Checking log consistency at PrevLogIndex=%d\n", req.PrevLogIndex)

		if len(node.Log) < req.PrevLogIndex {
			fmt.Println("Rejecting request: Log is too short, missing previous entry. ", node.CommitIndex, req.PrevLogIndex)
			resp := AppendEntriesResponse{Term: node.CurrentTerm, Success: false, PrevLogIndex: node.CommitIndex}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}

		if node.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			fmt.Println("Rejecting request: Log term mismatch at PrevLogIndex.")
			resp := AppendEntriesResponse{Term: node.CurrentTerm, Success: false, PrevLogIndex: node.CommitIndex}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
	}

	// Append any new entries that are not already in the log.
	fmt.Println("Appending new entries if necessary")
	newIndex := req.PrevLogIndex
	for i, entry := range req.Entries {
		if len(node.Log) > newIndex {
			if node.Log[newIndex].Term != entry.Term {
				// Conflict detected: delete the conflicting entry and all that follow.
				fmt.Printf("Conflict detected at index %d! Truncating log and appending new entries.\n", newIndex)
				node.Log = node.Log[:newIndex]
				node.Log = append(node.Log, req.Entries[i:]...)
				break
			}
		} else {
			fmt.Printf("Appending new entry at index %d: %+v\n", newIndex, entry)
			node.Log = append(node.Log, entry)
		}
		newIndex++
	}

	// If LeaderCommit > commitIndex, update commitIndex.
	if req.LeaderCommit > node.CommitIndex {
		prevCommitIndex := node.CommitIndex
		if req.LeaderCommit < len(node.Log) {
			node.CommitIndex = req.LeaderCommit
		} else {
			node.CommitIndex = len(node.Log)
		}
		fmt.Printf("Updated commit index from %d to %d\n", prevCommitIndex, node.CommitIndex)
	}

	fmt.Println("AppendEntries request processed successfully")
	fmt.Println("current node log is ----- \n ", node.Log)
	resp := AppendEntriesResponse{Term: node.CurrentTerm, Success: true}
	w.Header().Set("Content-Type", "application/json")
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
		fmt.Println("response from append entries", response)
		fmt.Println("leader logs are ", node.Log)
		return &response, nil
	}
	return response, nil
}
