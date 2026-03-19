#!/bin/bash
# Generate Go code from proto files

# Registry
protoc -I. --go_out=. --go_opt=module=locator --go-grpc_out=. \
    --go-grpc_opt=module=locator api/registry/registry.proto

protoc -I. --go_out=. --go_opt=module=locator api/registry/service.proto

# Transport
protoc -I. --go_out=. --go_opt=module=locator --go-grpc_out=. \
    --go-grpc_opt=module=locator api/transport/transport.proto

echo "Proto generation completed."
