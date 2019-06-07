default: vet test

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test ./... -race

bench:
	go test ./... -run=NONE -bench=. -benchmem

bench-race:
	go test ./... -run=NONE -bench=. -race

deps:
	dep ensure -v

errcheck:
	errcheck ./...

.PHONY: vet test deps errcheck
