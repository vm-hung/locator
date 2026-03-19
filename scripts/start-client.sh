#!/bin/bash

# Start hello service
export GRPC_XDS_BOOTSTRAP=$PWD/bootstrap.json
go run ./example/hello/cmd/client --address xds:///hello --name World