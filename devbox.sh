#!/usr/bin/env bash

set -euo pipefail

build() {
    OUTPUT="${OUTPUT:-build/azqueuegw}"
    go build -o "$OUTPUT" ./cmd/azqueuegw
}

test() {
    CGO_ENABLED=1 go test -race -timeout 30s -tags dev ./...
}

lint() {
    golangci-lint run --timeout 10m
}

eval $@
