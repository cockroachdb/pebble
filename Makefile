GO := go
GOFLAGS :=
PKG := ./...
BENCH_PKGS := internal/arenaskl internal/batchskl internal/record sstable .
STRESSFLAGS :=
TESTS := .
BUILDER := ../../cockroachdb/cockroach/build/builder.sh

.PHONY: all
all:
	@echo usage:
	@echo "  make test"
	@echo "  make testrace"
	@echo "  make stress"
	@echo "  make stressrace"
	@echo "  make bench"
	@echo "  make mod-update"
	@echo "  make clean"

.PHONY: test
test:
	${GO} test ${GOFLAGS} -run ${TESTS} ${PKG}

.PHONY: test
test-linux:
	${BUILDER} ${GO} test ${GOFLAGS} -run ${TESTS} $(shell go list ${PKG})

.PHONY: testrace
testrace: GOFLAGS += -race
testrace: test

.PHONY: stress stressrace
stressrace: GOFLAGS += -race
stress stressrace:
	${GO} test -v ${GOFLAGS} -exec 'stress ${STRESSFLAGS}' -run "${TESTS}" -timeout 0 ${PKG}

.PHONY: bench
bench: GOFLAGS += -timeout 1h
bench: $(patsubst %,%.bench,$(if $(findstring ./...,${PKG}),${BENCH_PKGS},${PKG}))

internal/arenaskl.bench: GOFLAGS += -cpu 1,8

%.bench:
	${GO} test -run - -bench . -count 10 ${GOFLAGS} ./$* 2>&1 | tee $*/bench.txt.new

# The cmd/pebble/rocksdb.go file causes various cockroach dependencies
# to be pulled in which is undesirable. Hack around this by
# temporarily moving hiding that file.
mod-update:
	mv cmd/pebble/rocksdb.go cmd/pebble/_rocksdb.go
	GO111MODULE=on go mod vendor
	mv cmd/pebble/_rocksdb.go cmd/pebble/rocksdb.go

.PHONY: clean
clean:
	rm -f $(patsubst %,%.test,$(notdir $(shell go list ${PKG})))
