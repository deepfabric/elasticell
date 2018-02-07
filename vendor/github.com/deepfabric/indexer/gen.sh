#!/bin/bash
#
# Generate all indexer protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
DIRS="./cql ./wal/walpb"

GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

for dir in ${DIRS}; do
	pushd ${dir}
		protoc --gofast_out=. -I=.:"${GOGOPROTO_PATH}" *.proto
		sed -i.bak -E 's/import _ \"gogoproto\"//g' *.pb.go
		rm -f *.bak
	popd
done
