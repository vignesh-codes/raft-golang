package main

import (
	"fmt"
	"net/http"
)

func handleNewCommandEntry(w http.ResponseWriter, r *http.Request) {
	fmt.Println("received new command")
	var entry = LogEntry{Command: "new command", Term: node.CurrentTerm, Index: len(node.Log)}
	node.mu.Lock()
	defer node.mu.Unlock()
	node.Log = append(node.Log, entry)
	// lock myNewEntries to avoid concurrent access
	myNewEntries.Mutex.Lock()
	defer myNewEntries.Mutex.Unlock()
	myNewEntries.Entries = append(myNewEntries.Entries, entry)
	fmt.Println("log is now", node.Log)
	w.WriteHeader(http.StatusOK)
}
