#!/bin/bash

# Define common peer list
PEERS="1@localhost:9991,2@localhost:9992,3@localhost:9993"

# Start Node 1
go run ./example/locator --id 1 --peers $PEERS \
    --transport-host 127.0.0.1 --transport-port 9991 \
    --registry-host 0.0.0.0 --registry-port 8881 \
    --data-dir ./data/node1 &

# Start Node 2
go run ./example/locator --id 2 --peers $PEERS \
    --transport-host 127.0.0.1 --transport-port 9992 \
    --registry-host 0.0.0.0 --registry-port 8882 \
    --data-dir ./data/node2 &

# Start Node 3
go run ./example/locator --id 3 --peers $PEERS \
    --transport-host 127.0.0.1 --transport-port 9993 \
    --registry-host 0.0.0.0 --registry-port 8883 \
    --data-dir ./data/node3 &

# Wait for all background processes
wait