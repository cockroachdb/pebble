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

override testflags :=
.PHONY: test
test:
	${GO} test -mod=vendor -tags '$(TAGS)' ${testflags} -run ${TESTS} ${PKG}

.PHONY: testrace
testrace: testflags += -race
testrace: test

.PHONY: stress stressrace
stressrace: testflags += -race
stress stressrace: testflags += -exec 'stress ${STRESSFLAGS}' -timeout 0 -test.v
stress stressrace: test

.PHONY: stressmeta
stressmeta: override PKG = ./internal/metamorphic
stressmeta: override STRESSFLAGS += -p 1
stressmeta: override TESTS = TestMeta$$
stressmeta: stress

.PHONY: generate
generate:
	${GO} generate -mod=vendor ${PKG}

mod-update:
	${GO} get -u
	${GO} mod tidy
	${GO} mod vendor

.PHONY: clean
clean:
	rm -f $(patsubst %,%.test,$(notdir $(shell go list ${PKG})))
