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

git_dirty := $(shell git status -s)

.PHONY: git-clean-check
git-clean-check:
ifneq ($(git_dirty),)
	@echo "Git repository is dirty!"
	@false
else
	@echo "Git repository is clean."
endif

.PHONY: mod-tidy-check
mod-tidy-check:
ifneq ($(git_dirty),)
	$(error mod-tidy-check must be invoked on a clean repository)
endif
	@${GO} mod tidy
	$(MAKE) git-clean-check

.PHONY: format
format:
	for _file in $$(gofmt -s -l . | grep -vE '^vendor/'); do \
		gofmt -s -w $$_file ; \
	done

.PHONY: format-check
format-check:
ifneq ($(git_dirty),)
	$(error format-check must be invoked on a clean repository)
endif
	$(MAKE) format
	$(MAKE) git-clean-check
