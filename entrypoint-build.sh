#/bin/bash

SRC=/go/src/github.com/deepfabric
DIST_DIR=/apps/deepfabric/dist
TARGET=$ELASTICELL_BUILD_TARGET
VERSION=$ELASTICELL_BUILD_VERSION

clone_all() {
    clone_elasticell
    clone_elasticell_proxy
}

clone_elasticell() {
    cd $SRC
    git clone https://github.com/deepfabric/elasticell.git
    echo "elasticell clone complete."
    cd $SRC/elasticell
    git checkout $VERSION
    echo "elasticell checkout to: $VERSION."
}

clone_elasticell_proxy() {
    cd $SRC
    git clone https://github.com/deepfabric/elasticell-proxy.git
    echo "elasticell-proxy clone complete."
    cd $SRC/elasticell-proxy
    git checkout $VERSION
    echo "elasticell-proxy checkout to: $VERSION."
}

build_proxy() {
    echo "start to build elasticell-proxy."
    cd $SRC/elasticell-proxy/cmd/redis
    go build -ldflags "-w -s" redis-proxy.go
    echo "complete build elasticell-proxy."
    chmod +x ./redis-proxy
    mv ./redis-proxy $DIST_DIR
}

build_pd() {
    echo "start to build pd."
    cd $SRC/elasticell/cmd/pd
    go build -ldflags "-w -s" pd.go
    echo "complete build pd."
    chmod +x ./pd
    mv ./pd $DIST_DIR
}

build_cell() {
    echo "start to build cell."
    cd $SRC/elasticell/cmd/cell
    go build -ldflags "-w -s" cell.go
    echo "complete build cell."
    chmod +x ./cell
    mv ./cell $DIST_DIR
}

build_all() {
    build_proxy
    build_pd
    build_cell
}

if [ "$VERSION" = "" ]; then
    VERSION="master"
fi

DIST_DIR=$DIST_DIR/$VERSION
mkdir -p $DIST_DIR

echo "build target: $TARGET"
echo "build version: $VERSION"
echo "build dist: $DIST_DIR"

if [ "$TARGET" = "pd" ]; then
    clone_elasticell
    build_pd
elif [ "$TARGET" = "cell" ]; then
    clone_elasticell
    build_cell
elif [ "$TARGET" = "proxy" ]; then
    clone_all
    build_proxy
elif [ "$TARGET" = "all" ]; then
    clone_all
    build_all
fi