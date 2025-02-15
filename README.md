# raft-golang

This program just registers the new node to the leader \
update-1: added leader election and heartbeat mechanisms

How to Run
Start the first node (Leader)

```sh
go run raft_node.go 8000
```
This node becomes the leader.
Start a second node and join the first

```sh
go run raft_node.go 8001 http://localhost:8000
```
This node registers with 8000 and syncs leader and peer info.
Start a third node and join via any existing node

```sh
go run raft_node.go 8002 http://localhost:8001
```
This node gets leader info and full cluster details from 8001.
