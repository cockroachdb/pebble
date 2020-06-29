GO := go
PKG := ./...
GOFLAGS :=
STRESSFLAGS :=
TAGS := invariants
TESTS := .

.PHONY: all
all:
	@echo usage:
	@echo "  make test"
	@echo "  make testrace"
	@echo "  make stress"
	@echo "  make stressrace"
	@echo "  make stressmeta"
	@echo "  make mod-update"
	@echo "  make clean"

.PHONY: test
test:
	${GO} test -mod=vendor -tags '$(TAGS)' ${GOFLAGS} -run ${TESTS} ${PKG}

.PHONY: testrace
testrace: GOFLAGS += -race
testrace: test

.PHONY: stress stressrace
stressrace: GOFLAGS += -race
stress stressrace:
	${GO} test -mod=vendor -v -tags '$(TAGS)' ${GOFLAGS} -exec 'stress ${STRESSFLAGS}' -run '${TESTS}' -timeout 0 ${PKG}

.PHONY: stressmeta
stressmeta: PKG = ./internal/metamorphic
stressmeta: STRESSFLAGS += -p 1
stressmeta: TESTS = TestMeta$$
stressmeta:
	${GO} test -mod=vendor -v -tags '$(TAGS)' ${GOFLAGS} -exec 'stress ${STRESSFLAGS}' -run '${TESTS}' -timeout 0 ${PKG}

.PHONY: generate
generate:
	${GO} generate -mod=vendor ${PKG}

# The cmd/pebble/{badger,boltdb,rocksdb}.go files causes various
# cockroach dependencies to be pulled in which is undesirable. Hack
# around this by temporarily moving hiding that file.
mod-update:
	mkdir -p cmd/pebble/_bak
	mv cmd/pebble/{badger,boltdb,rocksdb}.go cmd/pebble/_bak
	${GO} get -u
	${GO} mod tidy
	${GO} mod vendor
	mv cmd/pebble/_bak/* cmd/pebble && rmdir cmd/pebble/_bak

.PHONY: clean
clean:
	rm -f $(patsubst %,%.test,$(notdir $(shell go list ${PKG})))
