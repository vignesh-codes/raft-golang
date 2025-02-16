package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// syncPeers broadcasts the current peer list and leader info to all peers.
func syncPeers() {
	node.mu.Lock()
	update := JoinResponse{Leader: node.Leader, Peers: node.Peers}
	data, err := json.Marshal(update)
	node.mu.Unlock()
	if err != nil {
		log.Println("Error marshalling sync data:", err)
		return
	}

	for _, peer := range node.Peers {
		if peer.ID == node.ID {
			continue
		}
		go func(peer PeerNode) {
			_, err := http.Post(peer.Address+"/sync", "application/json", bytes.NewBuffer(data))
			if err != nil {
				log.Printf("Failed to sync with peer %s: %v", peer.Address, err)
			}
		}(peer)
	}
}

// handleSync updates the node's peer list and leader info.
func handleSync(w http.ResponseWriter, r *http.Request) {
	var update JoinResponse
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		http.Error(w, "Invalid sync request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	node.Peers = update.Peers
	node.Leader = update.Leader
	node.mu.Unlock()

	log.Printf("Synced with leader: %s and %d peers", node.Leader.Address, len(node.Peers))
}

// handleHeartbeat simply resets the election timer.
func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	electionResetChan <- true
	w.WriteHeader(http.StatusOK)
}

// sendHeartbeats (or log replication) is used by the leader to send AppendEntries RPCs.
func sendHeartbeats() {
	for {
		node.mu.Lock()
		if node.State != Leader {
			node.mu.Unlock()
			return
		}

		// Get the previous log index and term.
		var prevLogIndex, prevLogTerm int
		if len(node.Log) > 0 {
			prevLogIndex = len(node.Log) - 1
			prevLogTerm = node.Log[prevLogIndex].Term
		}

		// Send new log entries instead of an empty heartbeat.
		// lock myNewEntries to avoid concurrent access
		myNewEntries.Mutex.Lock()
		entries := make([]LogEntry, len(myNewEntries.Entries))
		copy(entries, myNewEntries.Entries)
		myNewEntries.Entries = myNewEntries.Entries[:0] // Clear the slice after copying
		myNewEntries.Mutex.Unlock()

		req := AppendEntriesRequest{
			Term:         node.CurrentTerm,
			LeaderID:     node.ID,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries, // Includes new log entries
			LeaderCommit: node.CommitIndex,
		}

		fmt.Println("Leader sending AppendEntries to peers:", node.Peers)

		// Copy peers list to avoid concurrent map access.
		peersCopy := make([]PeerNode, 0, len(node.Peers))
		for _, p := range node.Peers {
			peersCopy = append(peersCopy, p)
		}
		node.mu.Unlock()

		// Send heartbeats in parallel
		for _, peer := range peersCopy {
			if peer.ID == node.ID {
				continue
			}

			go func(peer PeerNode) {
				resp, err := sendAppendEntries(peer, req)
				if err != nil || !resp.Success {
					log.Printf("Failed to send AppendEntries to %s: %v", peer.Address, err)
					if err != nil {
						node.mu.Lock()
						delete(node.Peers, peer.ID)
						fmt.Println("Updated peers list:", node.Peers)
						node.mu.Unlock()
						syncPeers()
					}
				}
			}(peer)
		}

		time.Sleep(5 * time.Second) // Maintain heartbeat interval
	}
}

// startLeaderCheck monitors for heartbeat timeouts and triggers an election.
func startLeaderCheck() {
	for {
		select {
		case <-electionResetChan:
			// Election timer reset.
		case <-time.After(electionTimeout):
			log.Println("Leader timeout! Starting election...")
			startElection()
		}
	}
}
