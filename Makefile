PKG = ./...
GOFLAGS =
TESTS = .

.PHONY: all
all:
	@echo usage:
	@echo "  make stress"
	@echo "  make clean"

.PHONY: stress
stress: $(patsubst %,%.stress,$(shell go list ${PKG}))

.PHONY: stressrace
stressrace: GOFLAGS += -race
stressrace: stress

%.stress:
	go test ${GOFLAGS} -i -v -c $*
	stress -maxfails 1 ./$(*F).test -test.run ${TESTS}

.PHONY: clean
clean:
	rm -f $(patsubst %,%.test,$(notdir $(shell go list ${PKG})))
