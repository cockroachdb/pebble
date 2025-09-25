GO := go
PKG := ./...
GOFLAGS :=
STRESSFLAGS :=
TAGS := invariants
TESTS := .
COVER_PROFILE := coverprofile.out

.PHONY: all
all:
	@echo usage:
	@echo "  make test"
	@echo "  make testrace"
	@echo "  make stress"
	@echo "  make stressrace"
	@echo "  make stressmeta"
	@echo "  make crossversion-meta"
	@echo "  make testcoverage"
	@echo "  make mod-update"
	@echo "  make gen-bazel"
	@echo "  make generate"
	@echo "  make generate-test-data"
	@echo "  make clean"
override testflags :=
.PHONY: test
test:
	${GO} test -tags '$(TAGS)' ${testflags} -run ${TESTS} ${PKG}

.PHONY: testcoverage
testcoverage:
	${GO} test -tags '$(TAGS)' ${testflags} -run ${TESTS} ${PKG} -coverprofile ${COVER_PROFILE}

.PHONY: testrace
testrace: testflags += -race -timeout 20m
testrace: test

.PHONY: testasan
testasan: testflags += -asan -timeout 20m
testasan: TAGS += slowbuild
testasan:
	ASAN_OPTIONS=detect_leaks=0 ${GO} test -tags '$(TAGS)' ${testflags} -run ${TESTS} ${PKG}

.PHONY: testmsan
testmsan: export CC=clang
testmsan: testflags += -msan -timeout 20m
testmsan: TAGS += slowbuild
testmsan: test

.PHONY: testnocgo
testnocgo:
	CGO_ENABLED=0 ${GO} test -tags '$(TAGS)' ${testflags} -run ${TESTS} ${PKG}

.PHONY: testobjiotracing
testobjiotracing:
	${GO} test -tags '$(TAGS) pebble_obj_io_tracing' ${testflags} -run ${TESTS} ./objstorage/objstorageprovider/objiotracing

.PHONY: lint
lint:
	${GO} test -tags '$(TAGS)' ${testflags} -run ${TESTS} ./internal/lint

.PHONY: stress stressrace
stressrace: testflags += -race
stress stressrace: testflags += -exec 'stress ${STRESSFLAGS}' -timeout 0 -test.v
stress stressrace: test

.PHONY: stressmeta
stressmeta: override PKG = ./internal/metamorphic
stressmeta: override STRESSFLAGS += -p 1
stressmeta: override TESTS = TestMeta$$
stressmeta: stress

.PHONY: crossversion-meta
crossversion-meta: LATEST_RELEASE := crl-release-25.3
crossversion-meta:
	git checkout ${LATEST_RELEASE}; \
		${GO} test -c ./internal/metamorphic -o './internal/metamorphic/crossversion/${LATEST_RELEASE}.test'; \
		git checkout -; \
		${GO} test -c ./internal/metamorphic -o './internal/metamorphic/crossversion/head.test'; \
		${GO} test -tags '$(TAGS)' ${testflags} -v -timeout 20m -run 'TestMetaCrossVersion' ./internal/metamorphic/crossversion --version '${LATEST_RELEASE},${LATEST_RELEASE},${LATEST_RELEASE}.test' --version 'HEAD,HEAD,./head.test'

.PHONY: stress-crossversion
stress-crossversion:
	STRESS=1 ./scripts/run-crossversion-meta.sh crl-release-24.1 crl-release-24.3 crl-release-25.1 crl-release-25.2 crl-release-25.3 crl-release-25.4

.PHONY: test-s390x-qemu
test-s390x-qemu: TAGS += slowbuild
test-s390x-qemu: S390X_GOVERSION := 1.23
test-s390x-qemu:
	@echo "Running tests on s390x using QEMU"
	@echo "Requires a recent linux with docker and qemu-user-static installed"
	@echo "(sudo apt-get install -y qemu-user-static)"
	@echo ""
	@qemu-s390x-static --version
	@echo ""
	@docker run --rm -v "$(CURDIR):/pebble" --platform=linux/s390x golang:${S390X_GOVERSION} \
		bash -c " \
				uname -a && \
				lscpu | grep Endian && \
				cd /pebble && \
				go version && \
				go test -tags '$(TAGS)' -timeout 30m ./..."

.PHONY: gen-bazel
gen-bazel:
	@echo "Generating WORKSPACE"
	@echo 'workspace(name = "com_github_cockroachdb_pebble")' > WORKSPACE
	@echo 'Running gazelle...'
	${GO} run github.com/bazelbuild/bazel-gazelle/cmd/gazelle@v0.37.0 update --go_prefix=github.com/cockroachdb/pebble --repo_root=.
	@echo 'You should now be able to build Cockroach using:'
	@echo '  ./dev build short -- --override_repository=com_github_cockroachdb_pebble=${CURDIR}'

.PHONY: clean-bazel
clean-bazel:
	git clean -dxf WORKSPACE BUILD.bazel '**/BUILD.bazel'

.PHONY: generate
generate:
	${GO} generate ${PKG}

generate:

# Note that the output of generate-test-data is not deterministic. This should
# only be run manually as needed.
.PHONY: generate-test-data
generate-test-data:
	${GO} run -tags make_incorrect_manifests ./tool/make_incorrect_manifests.go
	${GO} run -tags make_test_find_db ./tool/make_test_find_db.go
	${GO} run -tags make_test_sstables ./tool/make_test_sstables.go
	${GO} run -tags make_test_remotecat ./tool/make_test_remotecat.go

.PHONY: mod-update
mod-update:
	${GO} get -u
	${GO} mod tidy

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
	go install -C internal/devtools github.com/cockroachdb/crlfmt && crlfmt -w -tab 2 .

.PHONY: format-check
format-check:
ifneq ($(git_dirty),)
	$(error format-check must be invoked on a clean repository)
endif
	$(MAKE) format
	git diff
	$(MAKE) git-clean-check
