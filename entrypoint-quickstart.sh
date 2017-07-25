#!/bin/bash

ELASTICELL_DIR=/apps/deepfabric
CFG_DIR=$ELASTICELL_DIR/cfg
LOG_DIR=$ELASTICELL_DIR/log

start_elasticell() {
    $ELASTICELL_DIR/$1 --cfg=$CFG_DIR/$1$2.json --log-file=$LOG_DIR/$1$2.log &
}

start_elasticell_proxy() {
    $ELASTICELL_DIR/redis-proxy --cfg=$CFG_DIR/proxy.json
}

echo "begin to start pd cluster"
start_elasticell pd 1
start_elasticell pd 2
start_elasticell pd 3
sleep 3
echo "pd cluster is started"

echo "begin to start cell cluster"
start_elasticell cell 1
sleep 2
start_elasticell cell 2
start_elasticell cell 3
echo "cell cluster is started"

echo "begin to start redis proxy"
start_elasticell_proxy
