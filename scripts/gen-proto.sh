#!/bin/bash
# Generate Go code from proto files

MODULE="github.com/hunkvm/locator"

# Types
protoc -I. --go_out=. --go_opt=module=${MODULE} protos/types/*.proto

# Registry
protoc -I. --go_out=. --go_opt=module=${MODULE} --go-grpc_out=. \
    --go-grpc_opt=module=${MODULE} protos/registry/*.proto

# Transport
protoc -I. --go_out=. --go_opt=module=${MODULE} --go-grpc_out=. \
    --go-grpc_opt=module=${MODULE} protos/transport/*.proto

echo "Proto generation completed."
