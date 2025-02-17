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
		fmt.Println("Sending heartbeats with term", node.CurrentTerm)

		node.mu.Lock()
		if node.State != Leader {
			node.mu.Unlock()
			return
		}

		peersCopy := make([]PeerNode, 0, len(node.Peers))
		for _, p := range node.Peers {
			peersCopy = append(peersCopy, p)
		}
		node.mu.Unlock()

		for _, peer := range peersCopy {
			if peer.ID == node.ID {
				continue
			}

			// go func(peer PeerNode) {
			node.mu.Lock()
			nextIdx, exists := node.NextIndex[peer.ID]
			if !exists {
				node.NextIndex[peer.ID] = len(node.Log)
				nextIdx = len(node.Log)
			}
			node.mu.Unlock()

			if nextIdx > len(node.Log) {
				return
			}

			var prevLogIndex, prevLogTerm int
			if nextIdx > 0 {
				prevLogIndex = nextIdx - 1
				prevLogTerm = node.Log[prevLogIndex].Term
			}

			entries := node.Log[nextIdx:]

			req := AppendEntriesRequest{
				Term:         node.CurrentTerm,
				LeaderID:     node.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: node.CommitIndex,
			}

			resp, err := sendAppendEntries(peer, req)
			if err != nil || !resp.Success {
				log.Printf("Failed to send AppendEntries to %s: %v", peer.Address, err)
				node.mu.Lock()
				if node.NextIndex[peer.ID] > 0 {
					node.NextIndex[peer.ID]-- // Back off
				}
				node.mu.Unlock()
			} else {
				node.mu.Lock()
				node.NextIndex[peer.ID] = nextIdx + len(entries)
				node.mu.Unlock()
			}
			// }(peer)
		}

		time.Sleep(5 * time.Second)
	}
}

// startLeaderCheck monitors for heartbeat timeouts and triggers an election.
func startLeaderCheck() {
	for {
		select {
		case <-electionResetChan:
			// Election timer reset.
		case <-time.After(electionTimeout):
			if node.State != Leader {
				log.Println("Leader timeout! Starting election...")
				startElection()
			}

		}
	}
}
