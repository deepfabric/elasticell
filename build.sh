#!/bin/bash
DIST=/apps/deepfabric/dist

pull() {
    docker pull deepfabric/elasticell-build
}

mk_dist() {
    rm -rf $DIST
    rm -rf ./dist
    mkdir -p $DIST
    mkdir ./dist
}

build_binary() {
    docker run -it --rm -v $DIST:/apps/deepfabric/dist -e ELASTICELL_BUILD_TARGET=all -e ELASTICELL_BUILD_VERSION=$1 deepfabric/elasticell-build
    mv $DIST/$1/* ./dist
    rm -rf $DIST
}

build_image() {
    docker build -t deepfabric/$2:$1 -f ./Dockerfile-$2 .
    docker tag deepfabric/$2:$1 deepfabric/$2
}

pull
mk_dist
build_binary $1
build_image $1 pd
build_image $1 cell
build_image $1 proxy
build_image $1 quickstart
