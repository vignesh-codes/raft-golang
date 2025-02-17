package main

import (
	"fmt"
	"net/http"
	"sync"
)

// LogEntryNode represents a node in the doubly linked list.
type LogEntryNode struct {
	Entry LogEntry
	Prev  *LogEntryNode
	Next  *LogEntryNode
}

// NewEntries represents the doubly linked list with length tracking.
type NewEntries struct {
	Head   *LogEntryNode
	Tail   *LogEntryNode
	Length int        // Track the number of nodes in the list
	Mutex  sync.Mutex // Synchronization for concurrent access
}

// Initialize an empty doubly linked list for new log entries.
var myNewEntries = NewEntries{}

// AddLogEntry adds a new entry to the end of the linked list.
func (ne *NewEntries) AddLogEntry(entry LogEntry) {
	ne.Mutex.Lock()
	defer ne.Mutex.Unlock()

	newNode := &LogEntryNode{Entry: entry}

	// If list is empty, set head and tail to new node.
	if ne.Head == nil {
		ne.Head = newNode
		ne.Tail = newNode
	} else {
		// Append to the end of the list.
		ne.Tail.Next = newNode
		newNode.Prev = ne.Tail
		ne.Tail = newNode
	}

	ne.Length++ // Increment length
	fmt.Println("Added new entry:", entry, "| Total entries:", ne.Length)
}

// DeleteLogEntry removes an entry from the doubly linked list based on index.
func (ne *NewEntries) DeleteLogEntry(index int) {
	ne.Mutex.Lock()
	defer ne.Mutex.Unlock()

	curr := ne.Head
	for curr != nil {
		if curr.Entry.Index == index {
			// If removing the head node
			if curr.Prev == nil {
				ne.Head = curr.Next
				if ne.Head != nil {
					ne.Head.Prev = nil
				}
			} else {
				curr.Prev.Next = curr.Next
			}

			// If removing the tail node
			if curr.Next == nil {
				ne.Tail = curr.Prev
				if ne.Tail != nil {
					ne.Tail.Next = nil
				}
			} else {
				curr.Next.Prev = curr.Prev
			}

			ne.Length-- // Decrement length
			fmt.Println("Deleted entry with index:", index, "| Total entries:", ne.Length)
			return
		}
		curr = curr.Next
	}
	fmt.Println("Entry not found for deletion:", index)
}

// GetLength returns the current number of log entries.
func (ne *NewEntries) GetLength() int {
	ne.Mutex.Lock()
	defer ne.Mutex.Unlock()
	fmt.Println("len is ", ne.Length)
	return ne.Length
}

// PrintEntries prints the log entries for debugging.
func (ne *NewEntries) GetEntries() []LogEntry {
	ne.Mutex.Lock()
	defer ne.Mutex.Unlock()

	var entries []LogEntry
	curr := ne.Head
	for curr != nil {
		entries = append(entries, curr.Entry)
		curr = curr.Next
	}
	return entries
}

func (ne *NewEntries) GetHeadAndDeleteHead() []LogEntry {
	ne.Mutex.Lock()
	defer ne.Mutex.Unlock()

	if ne.Head == nil {
		return nil // No entries to return
	}

	// Collect entries from the first node
	entries := []LogEntry{ne.Head.Entry}

	// Move head to the next node
	ne.Head = ne.Head.Next

	// If new head is not nil, update its Prev pointer to nil
	if ne.Head != nil {
		ne.Head.Prev = nil
	} else {
		// If the list becomes empty, update Tail as well
		ne.Tail = nil
	}

	return entries
}

func handleNewCommandEntry(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received new command")
	entry := LogEntry{Command: "new command", Term: node.CurrentTerm, Index: len(node.Log)}

	node.mu.Lock()
	defer node.mu.Unlock()
	node.Log = append(node.Log, entry)
	fmt.Println("adding new command to log", node.Log)
	// Add new entry to the doubly linked list
	myNewEntries.AddLogEntry(entry)

	fmt.Println("Log is now", node.Log)
	myNewEntries.GetEntries() // Debugging

	w.WriteHeader(http.StatusOK)
}
