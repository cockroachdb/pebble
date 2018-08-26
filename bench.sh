#!/bin/bash

# pushd arenaskl
# go test -cpu 1,8 -run - -bench . -count 10 -timeout 1h 2>&1 | tee bench.txt.new
# popd

# pushd batchskl
# go test -run - -bench . -count 10 -timeout 1h 2>&1 | tee bench.txt.new
# popd

pushd record
go test -run - -bench . -count 10 -timeout 1h 2>&1 | tee bench.txt.new
popd

pushd sstable
go test -run - -bench . -count 10 -timeout 1h 2>&1 | tee bench.txt.new
popd

go test -run - -bench . -count 10 -timeout 1h 2>&1 | tee bench.txt.new
