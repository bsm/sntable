default: vet test

vet:
	go vet ./...

test:
	go test ./...

.PHONY: vet test

# go get -u github.com/davelondon/rebecca/cmd/becca
README.md: README.md.tpl
	becca -package github.com/bsm/sntable
