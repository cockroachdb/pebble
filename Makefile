GO := go
PKG := ...
RACE :=
BAZELFLAGS :=
STRESSFLAGS :=
TESTS := .

.PHONY: all
all:
	@echo usage:
	@echo "  make build"
	@echo "  make test"
	@echo "  make testrace"
	@echo "  make stress"
	@echo "  make stressrace"
	@echo "  make update"
	@echo "  make mod-update"
	@echo "  make clean"

.PHONY: build
build:
	bazel build ${PKG}

.PHONY: test testrace stress stressrace
testrace stressrace: BAZELFLAGS += --features=race
test testrace:
	bazel test ${BAZELFLAGS} --test_filter=${TESTS} -- ${PKG}

stress stressrace: BAZELFLAGS += --config=stress
stress stressrace:
	bazel test ${BAZELFLAGS} --test_filter=${TESTS} -- ${PKG}

# TODO(peter): this should probably be a Bazel genrule.
.PHONY: generate
generate:
	GO111MODULE=off ${GO} generate ${PKG}

# Requires gazelle: go get -u github.com/bazelbuild/bazel-gazelle/cmd/gazelle
.PHONY: update
update:
	gazelle update

# The cmd/pebble/{badger,boltdb,rocksdb}.go files causes various
# cockroach dependencies to be pulled in which is undesirable. Hack
# around this by temporarily moving hiding that file.
.PHONY: mod-update
mod-update:
	mkdir -p cmd/pebble/_bak
	mv cmd/pebble/{badger,boltdb,rocksdb}.go cmd/pebble/_bak
	GO111MODULE=on ${GO} mod vendor
	mv cmd/pebble/_bak/* cmd/pebble && rmdir cmd/pebble/_bak

.PHONY: clean
clean:
	bazel clean
