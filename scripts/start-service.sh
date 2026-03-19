#!/bin/bash

# Start hello service
go run ./example/cmd/server --registry localhost:8881 \
    --address localhost:8089