package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

// --------------------- Cluster Join and Sync ---------------------

func handleJoin(w http.ResponseWriter, r *http.Request) {
	var newNode PeerNode
	if err := json.NewDecoder(r.Body).Decode(&newNode); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	node.mu.Lock()
	node.Peers[newNode.ID] = newNode
	log.Printf("New node joined: %s", newNode.Address)
	node.mu.Unlock()
	response := JoinResponse{Leader: node.Leader, Peers: node.Peers, CurrentTerm: node.CurrentTerm}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	if node.State == Leader {
		go syncPeers()
	}
}

func syncPeers() {
	// node.mu.Lock()
	update := JoinResponse{Leader: node.Leader, Peers: node.Peers, CurrentTerm: node.CurrentTerm}
	data, err := json.Marshal(update)
	// node.mu.Unlock()
	if err != nil {
		log.Println("Error marshalling sync data:", err)
		return
	}
	for _, peer := range node.Peers {
		if peer.ID == node.ID {
			continue
		}
		_, err := http.Post(peer.Address+"/sync", "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("Failed to sync with peer %s: %v", peer.Address, err)
		}
	}
}

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
	log.Printf("Synced with leader: %s, %d peers", node.Leader.Address, len(node.Peers))
}

// --------------------- HTTP Server ---------------------

func startServer(port string) {
	http.HandleFunc("/join", handleJoin)
	http.HandleFunc("/sync", handleSync)
	http.HandleFunc("/vote", handleVoteRequest)
	http.HandleFunc("/appendEntries", handleAppendEntries)
	http.HandleFunc("/newCommand", handleNewCommandEntry)
	http.HandleFunc("/status", getNodeStatus)
	// Additional endpoints as needed.
	log.Printf("Node %s listening on port %s...", node.ID, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func getNodeStatus(w http.ResponseWriter, r *http.Request) {
	node.mu.Lock()
	defer node.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(node)
}

func joinCluster(peerAddr string) {
	if !strings.HasPrefix(peerAddr, "http://") {
		peerAddr = "http://" + peerAddr
	}
	newNode := PeerNode{ID: node.ID, Address: node.Address}
	data, _ := json.Marshal(newNode)
	resp, err := http.Post(peerAddr+"/join", "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Fatalf("Failed to join cluster at %s: %v", peerAddr, err)
	}
	defer resp.Body.Close()
	var joinResp JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&joinResp); err != nil {
		log.Fatalf("Failed to parse join response: %v", err)
	}
	node.mu.Lock()
	node.Leader = joinResp.Leader
	node.Peers = joinResp.Peers
	node.CurrentTerm = joinResp.CurrentTerm
	node.mu.Unlock()
	log.Printf("Joined cluster. Leader: %s, Total peers: %d", node.Leader.Address, len(node.Peers))
}

// --------------------- Main ---------------------

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run raft_node.go <port> <optional: known peer>")
		return
	}
	port := os.Args[1]
	knownPeer := ""
	if len(os.Args) >= 3 {
		knownPeer = os.Args[2]
	}
	rand.Seed(time.Now().UnixNano())
	// Set election timeout to a random value between 8 and 12 seconds.
	electionTimeout = time.Duration(rand.Intn(4)+8) * time.Second

	node = RaftNode{
		ID:          "node-" + port,
		Address:     "http://localhost:" + port,
		Peers:       make(map[string]PeerNode),
		State:       Follower,
		Log:         make([]LogEntry, 0),
		NextIndex:   make(map[string]int),
		CommitIndex: 0,
	}
	// Add self to peers.
	node.Peers[node.ID] = PeerNode{ID: node.ID, Address: node.Address}

	go func() {
		// Start dead peer removal if desired (not shown here in full detail).
		// go removeDeadPeers()
	}()

	if knownPeer == "" {
		// Standalone node becomes leader.
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		startHeartbeats()
	} else {
		joinCluster(knownPeer)
		startLeaderCheck()
	}
	startServer(port)
}
