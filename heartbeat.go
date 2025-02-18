package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

var SENDHEARBEATSFLAG = false

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
		// go func(peer PeerNode) {
		_, err := http.Post(peer.Address+"/sync", "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Failed to sync with peer %s: %v", peer.Address, err)
		}
		// }(peer)
	}
}

// handleSync updates the node's peer list and leader info.
func handleSync(w http.ResponseWriter, r *http.Request) {
	var update JoinResponse
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		fmt.Println("err is ", err)
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

func startHeartbeats() {
	if quitHeartbeats != nil {
		return // Already running
	}
	quitHeartbeats = make(chan struct{})
	go sendHeartbeats()
	fmt.Println("Heartbeats started")
}

func stopHeartbeats() {
	if quitHeartbeats != nil {
		close(quitHeartbeats) // Send signal to stop goroutine
		quitHeartbeats = nil
		fmt.Println("Heartbeats stopped")
	}
}

func startLeaderCheck() {
	if quitLeaderCheck != nil {
		return // Already running
	}
	quitLeaderCheck = make(chan struct{})
	go LeaderCheck()
	fmt.Println("LeaderCheck Started")
}

func stopLeaderCheck() {
	if quitHeartbeats != nil {
		close(quitLeaderCheck) // Send signal to stop goroutine
		quitLeaderCheck = nil
		fmt.Println("LeaderCheck stopped")
	}
}

var quitHeartbeats chan struct{}  // Channel to control heartbeat execution
var quitLeaderCheck chan struct{} // Channel to control leader check execution

func sendHeartbeats() {
	for {
		select {
		case <-quitHeartbeats:
			fmt.Println("Stopping heartbeats...")
			return // Exit the function when a signal is received
		default:
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
			fmt.Println("sendding append entries to peers ", peersCopy)
			for _, peer := range peersCopy {
				if peer.ID == node.ID {
					continue
				}

				go func(peer PeerNode) {
					node.mu.Lock()
					nextIdx := node.NextIndex[peer.ID]
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
						Address:      node.Address,
					}

					resp, err := sendAppendEntries(peer, req)
					if err != nil || !resp.Success {
						fmt.Printf("Failed AppendEntries to %s, backing off\n", peer.ID)
						node.mu.Lock()
						node.NextIndex[peer.ID] = max(0, resp.LastLogIndex+1)
						node.mu.Unlock()
					} else {
						node.mu.Lock()
						node.NextIndex[peer.ID] = resp.LastLogIndex + 1
						node.mu.Unlock()
					}
				}(peer)
			}

			time.Sleep(5 * time.Second) // Heartbeat interval
		}
	}
}

// startLeaderCheck monitors for heartbeat timeouts and triggers an election.
func LeaderCheck() {
	for {
		select {
		case <-quitLeaderCheck:
			fmt.Println("Stopping LeaderChecks...")
			return // Exit the function when a signal is received
		default:
			select {
			case <-electionResetChan:
				fmt.Println("electionresetchan received ********* ", node.State)
				// Election timer reset.
			case <-time.After(electionTimeout):
				if node.State == Follower {
					log.Println("Leader timeout! Starting election...")
					stopLeaderCheck()
					startElection()
				}
			}
		}
	}
}
