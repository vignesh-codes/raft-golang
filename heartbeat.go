package main

import (
	"fmt"
	"log"
	"time"
)

// --------------------- Heartbeats and Leader Check ---------------------

var quitHeartbeats chan struct{}
var quitLeaderCheck chan struct{}

func startHeartbeats() {
	if quitHeartbeats != nil {
		fmt.Println("Heartbeats already running")
		return
	}
	quitHeartbeats = make(chan struct{})
	fmt.Println("Started heartbeats")
	go sendHeartbeats()
}

func stopHeartbeats() {
	if quitHeartbeats != nil {
		close(quitHeartbeats)
		quitHeartbeats = nil
	}
}

func sendHeartbeats() {
	for {
		select {
		case <-quitHeartbeats:
			return
		default:
			log.Printf("Sending heartbeats with term %d", node.CurrentTerm)
			log.Println("to peers ", node.Peers)
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
			time.Sleep(5 * time.Second)
		}
	}
}

func startLeaderCheck() {
	if quitLeaderCheck != nil {
		return
	}
	quitLeaderCheck = make(chan struct{})
	go leaderCheck()
}

func stopLeaderCheck() {
	if quitLeaderCheck != nil {
		close(quitLeaderCheck)
		quitLeaderCheck = nil
		fmt.Println("Stopped leader check")
	}
}

func leaderCheck() {
	for {
		select {
		case <-quitLeaderCheck:
			return
		case <-electionResetChan:
			// Election timer reset.
		case <-time.After(electionTimeout):
			node.mu.Lock()
			if node.State == Follower {
				log.Println("Leader timeout! Starting election...")
				node.mu.Unlock()
				startElection()
			} else {
				node.mu.Unlock()
			}
		}
	}
}
