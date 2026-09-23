SHELL := /bin/bash

.PHONY: all build test test-race vet lint test-integration test-walkthrough test-e2e coverage up down clean logs migrate-up migrate-down provision-queues

all: vet lint test-race

build:
	go build -o bin/wallet ./cmd/wallet

## Unit tests (no Docker needed).
test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...
	go vet -tags integration,e2e ./...

## golangci-lint (.golangci.yml), including integration and e2e test code.
lint:
	golangci-lint run ./...

## Integration tests: real PostgreSQL, LocalStack and Keycloak via testcontainers.
test-integration:
	go test -race -count=1 -tags integration ./test/integration/...

## Ordered business walkthrough with a visible result for each step.
test-walkthrough:
	go test -race -count=1 -timeout 10m -v -tags integration ./test/integration/... -run '^TestWalkthrough$$'

## End-to-end: the compiled binary as 3 independent processes + crash/restart.
test-e2e:
	go test -count=1 -timeout 15m -tags e2e ./test/e2e/...

## Coverage of unit + integration tests (Docker needed); prints the total.
coverage:
	go test -race -count=1 -tags integration -coverpkg=./cmd/...,./internal/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

up:
	docker compose up --build -d

## Stop the stack, keeping the database and queue volumes.
down:
	docker compose down

## Stop the stack and delete its volumes.
clean:
	docker compose down -v

logs:
	docker compose logs -f app-1 app-2 app-3

migrate-up:
	docker compose run --rm migrate migrate up

migrate-down:
	docker compose run --rm migrate migrate down 1

provision-queues:
	docker compose run --rm provision-queues provision-queues
