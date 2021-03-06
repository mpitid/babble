#!/bin/bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o babble/babble ../cmd/main.go
CGO_ENABLED=0 go build -o dummy/dummy ../cmd/dummy_client/main.go
docker build --no-cache=true -t babble babble/
docker build --no-cache=true -t dummy  dummy/