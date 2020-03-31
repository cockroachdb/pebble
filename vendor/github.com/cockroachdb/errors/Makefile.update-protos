# This makefile can be used to-regenerate the protobuf files.
#
# Prerequisites:
#   "protoc" from https://github.com/protocolbuffers/protobuf
#   go get github.com/cockroachdb/protoc-gen-gogoroach
#   go get github.com/gogo/protobuf/types
#   go get github.com/gogo/protobuf/protoc-gen-gogo

PROTOS := $(wildcard errorspb/*.proto)
GO_SOURCES = $(PROTOS:.proto=.pb.go)

SED = sed
SED_INPLACE := $(shell $(SED) --version 2>&1 | grep -q GNU && echo -i || echo "-i ''")

all: $(PROTOS)
	protoc \
	   -I$$GOPATH/src/ \
	   -I$$GOPATH/src/github.com \
	   -I$$GOPATH/src/github.com/gogo/protobuf \
	   -I$$GOPATH/src/github.com/gogo/protobuf/protobuf \
	  --gogoroach_out=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc,import_prefix=:. \
	  $(PROTOS:%=$(GOPATH)/src/github.com/cockroachdb/errors/%)
	mv -f github.com/cockroachdb/errors/errorspb/*.pb.go errorspb/
	$(SED) $(SED_INPLACE) -E \
		-e '/import _ /d' \
		-e 's!import (fmt|math) "github.com/(fmt|math)"! !g' \
		-e 's!github.com/((bytes|encoding/binary|errors|fmt|io|math|github\.com|(google\.)?golang\.org)([^a-z]|$$))!\1!g' \
		-e 's!golang.org/x/net/context!context!g' \
		$(GO_SOURCES)
	gofmt -s -w $(GO_SOURCES)
