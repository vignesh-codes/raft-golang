package main

import (
	"log"
	"net/http"
)

// --------------------- New Command Entry ---------------------

func handleNewCommandEntry(w http.ResponseWriter, r *http.Request) {
	log.Println("Received new command")
	entry := LogEntry{
		Command: "new command",
		Term:    node.CurrentTerm,
		Index:   len(node.Log),
	}
	node.mu.Lock()
	node.Log = append(node.Log, entry)
	// Update NextIndex for all peers.
	for id := range node.Peers {
		node.NextIndex[id] = len(node.Log)
	}
	node.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}
