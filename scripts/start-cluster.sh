#!/bin/bash

PIDS=()

cleanup() {
    printf "\r\033[2K"
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait
    exit 0
}

trap cleanup INT TERM

go run ./server/cmd --config ./server/config.n1.yml & PIDS+=($!)
go run ./server/cmd --config ./server/config.n2.yml & PIDS+=($!)
go run ./server/cmd --config ./server/config.n3.yml & PIDS+=($!)

wait