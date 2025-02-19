package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// --------------------- AppendEntries RPC Handler ---------------------

func handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	// Reset election timeout.
	electionResetChan <- true
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid append entries request", http.StatusBadRequest)
		return
	}
	// If we're leader and receive an AppendEntries with a strictly higher term, step down.
	if node.State == Leader && req.Term >= node.CurrentTerm {
		stepDownFromLeader(req.LeaderID, req.Address)
	}
	node.Leader = PeerNode{ID: req.LeaderID, Address: req.Address}
	node.mu.Lock()
	defer node.mu.Unlock()
	log.Printf("AppendEntries: Term=%d, LeaderID=%s, PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v, LeaderCommit=%d",
		req.Term, req.LeaderID, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)
	fmt.Println("Follower Logs ", node.Log)
	if req.Term < node.CurrentTerm {
		resp := AppendEntriesResponse{Term: node.CurrentTerm, Success: false, LastLogIndex: len(node.Log) - 1, CommitIndex: node.CommitIndex}
		json.NewEncoder(w).Encode(resp)
		return
	}
	// Check log consistency.
	if req.PrevLogIndex >= 1 {
		if len(node.Log) <= req.PrevLogIndex || node.Log[req.PrevLogIndex].Term != req.PrevLogTerm {
			resp := AppendEntriesResponse{
				Term:         node.CurrentTerm,
				Success:      false,
				LastLogIndex: len(node.Log) - 1,
				CommitIndex:  node.CommitIndex,
			}
			json.NewEncoder(w).Encode(resp)
			return
		}
	}
	// Append new entries.
	newIndex := req.PrevLogIndex + 1
	for i, entry := range req.Entries {
		if len(node.Log) > newIndex {
			if node.Log[newIndex].Term != entry.Term {
				node.Log = node.Log[:newIndex]
				node.Log = append(node.Log, req.Entries[i:]...)
				break
			}
		} else {
			node.Log = append(node.Log, entry)
		}
		newIndex++
	}
	// Update commit index.
	if req.LeaderCommit > node.CommitIndex {
		node.CommitIndex = min(req.LeaderCommit, len(node.Log)-1)
	}
	resp := AppendEntriesResponse{
		Term:         node.CurrentTerm,
		Success:      true,
		LastLogIndex: len(node.Log) - 1,
		CommitIndex:  node.CommitIndex,
	}
	json.NewEncoder(w).Encode(resp)
}

func sendAppendEntries(peer PeerNode, req AppendEntriesRequest) (*AppendEntriesResponse, error) {
	var response AppendEntriesResponse
	for i := 0; i < 3; i++ {
		data, _ := json.Marshal(req)
		resp, err := http.Post(peer.Address+"/appendEntries", "application/json", bytes.NewBuffer(data))
		if err != nil {
			time.Sleep(2 * time.Second)
			if i == 2 {
				delete(node.Peers, peer.ID)
				log.Printf("Removed peer %s", peer.ID)
				syncPeers()
			}
			continue
		}
		defer resp.Body.Close()
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		return &response, nil
	}
	return &response, fmt.Errorf("failed to send AppendEntries")
}
