#!/bin/bash
#
# Generate all elasticell protobuf bindings.
# Run from repository root.
#
set -e

# directories containing protos to be built
DIRS="./metapb ./pdpb ./mraft ./raftcmdpb ./errorpb"

GOGOPROTO_ROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

ELASTICELL_PB_PATH="${GOPATH}/src/github.com/deepfabric/elasticell/pkg/pb"


for dir in ${DIRS}; do
	pushd ${dir}
		protoc --gofast_out=plugins=grpc,import_prefix=github.com/deepfabric/elasticell/pkg/pb/:. -I=.:"${GOGOPROTO_PATH}":"${ELASTICELL_PB_PATH}":"${GOPATH}/src" *.proto
		sed -i.bak -E "s/github\.com\/deepfabric\/elasticell\/pkg\/pb\/(gogoproto|github\.com|golang\.org|google\.golang\.org)/\1/g" *.pb.go
		sed -i.bak -E 's/github\.com\/deepfabric\/elasticell\/pkg\/pb\/(errors|fmt|io|encoding\/binary|context)/\1/g' *.pb.go
		sed -i.bak -E 's/math \"github.com\/deepfabric\/elasticell\/pkg\/pb\/math\"//g' *.pb.go
        sed -i.bak -E 's/math_bits \"github.com\/deepfabric\/elasticell\/pkg\/pb\/math\/bits\"/math_bits \"math\/bits\"/g' *.pb.go
		sed -i.bak -E 's/_ \"gogoproto\"//g' *.pb.go
		sed -i.bak -E 's/fmt \"fmt\"//g' *.pb.go
		sed -i.bak -E 's/context \"context\"//g' *.pb.go
        sed -i.bak -E 's/io \"io\"//g' *.pb.go
		sed -i.bak -E 's/math \"math\"//g' *.pb.go
       
		rm -f *.bak
		goimports -w *.pb.go
	popd
done
