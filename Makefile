GO = go
GOFLAGS =
PKG = ./...
TESTS = .

.PHONY: all
all:
	@echo usage:
	@echo "  make test"
	@echo "  make testrace"
	@echo "  make stress"
	@echo "  make stressrace"
	@echo "  make bench"
	@echo "  make clean"

.PHONY: test
test:
	$(GO) test ${GOFLAGS} -run ${TESTS} ${PKG}

.PHONY: testrace
testrace: GOFLAGS += -race
testrace: test

.PHONY: stress
stress: $(patsubst %,%.stress,$(shell $(GO) list ${PKG}))

.PHONY: stressrace
stressrace: GOFLAGS += -race
stressrace: stress

%.stress:
	$(GO) test ${GOFLAGS} -i -v -c $*
	stress -maxfails 1 ./$(*F).test -test.run ${TESTS}

.PHONY: bench
bench: GOFLAGS += -timeout 1h
bench: $(patsubst %,%.bench,internal/arenaskl internal/batchskl internal/record sstable .)

internal/arenaskl.bench: GOFLAGS += -cpu 1,8

%.bench:
	$(GO) test -run - -bench . -count 10 ${GOFLAGS} ./$* 2>&1 | tee $*/bench.txt.new

.PHONY: clean
clean:
	rm -f $(patsubst %,%.test,$(notdir $(shell go list ${PKG})))
