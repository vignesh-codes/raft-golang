package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var node RaftNode

// Handle new node registration
func handleJoin(w http.ResponseWriter, r *http.Request) {
	var newNode PeerNode
	err := json.NewDecoder(r.Body).Decode(&newNode)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	// Add new node to the peer list
	node.Peers[newNode.ID] = newNode
	log.Printf("New node joined: %s", newNode.Address)

	// Create response with leader info and peers
	response := JoinResponse{
		Leader: node.Leader,
		Peers:  node.Peers,
	}

	// If leader, broadcast updated peers to all nodes
	if node.State == Leader {
		go syncPeers()
	}

	// Send leader info and peer list to the new node
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// Sync peers with all nodes
func syncPeers() {
	node.mu.Lock()
	defer node.mu.Unlock()

	peersJSON, _ := json.Marshal(node.Peers)

	for _, peer := range node.Peers {
		if peer.ID != node.ID { // Don't send to self
			go func(peer PeerNode) {
				resp, err := http.Post(peer.Address+"/sync-peers", "application/json", bytes.NewBuffer(peersJSON))
				if err != nil {
					log.Printf("Failed to sync peers with %s: %v", peer.Address, err)
				} else {
					resp.Body.Close()
				}
			}(peer)
		}
	}
}

// Handle peer synchronization request
func handleSyncPeers(w http.ResponseWriter, r *http.Request) {
	var updatedPeers map[string]PeerNode
	err := json.NewDecoder(r.Body).Decode(&updatedPeers)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	node.Peers = updatedPeers
	log.Println("Peers updated")

	w.WriteHeader(http.StatusOK)
}

// Start HTTP server for Raft communication
func startServer(port string) {
	http.HandleFunc("/join", handleJoin)
	http.HandleFunc("/sync-peers", handleSyncPeers)

	log.Printf("Node %s listening on port %s...", node.ID, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Register the node with a known peer
func joinCluster(peerAddress string) {
	newNode := PeerNode{ID: node.ID, Address: node.Address}
	newNodeJSON, _ := json.Marshal(newNode)

	resp, err := http.Post(peerAddress+"/join", "application/json", bytes.NewBuffer(newNodeJSON))
	if err != nil {
		log.Fatalf("Failed to join cluster at %s: %v", peerAddress, err)
	}
	defer resp.Body.Close()

	// Decode response to get leader info and peer list
	var joinResponse JoinResponse
	err = json.NewDecoder(resp.Body).Decode(&joinResponse)
	if err != nil {
		log.Fatalf("Failed to parse join response: %v", err)
	}

	// Update node with received leader info and peers
	node.mu.Lock()
	node.Leader = joinResponse.Leader
	node.Peers = joinResponse.Peers
	node.mu.Unlock()

	log.Printf("Successfully joined cluster. Leader: %s, Peers: %d", node.Leader.Address, len(node.Peers))
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run raft_node.go <port> <optional: known peer>")
		return
	}

	port := os.Args[1]
	knownPeer := ""
	if len(os.Args) == 3 {
		knownPeer = os.Args[2]
	}

	// Initialize Raft node
	node = RaftNode{
		ID:      "node-" + port,
		Address: "http://localhost:" + port,
		Peers:   make(map[string]PeerNode),
		State:   Follower,
	}

	// If first node, set itself as leader
	if knownPeer == "" {
		node.State = Leader
		node.Leader = PeerNode{ID: node.ID, Address: node.Address}
		log.Println("This node is the leader")
	} else {
		joinCluster(knownPeer)
	}

	// Start server
	startServer(port)
}
