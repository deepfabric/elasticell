#!/bin/bash

ELASTICELL_DIR=/apps/deepfabric
CFG_DIR=$ELASTICELL_DIR/cfg
LOG_DIR=$ELASTICELL_DIR/log

start_elasticell_pd() {
    $ELASTICELL_DIR/pd --cfg=$CFG_DIR/pd$1.json --log-file=$LOG_DIR/pd$1.log &
}

start_elasticell_cell() {
    $ELASTICELL_DIR/cell --log-file=$LOG_DIR/cell$1.log --pd=127.0.0.1:20801,127.0.0.1:20802,127.0.0.1:20803 --addr=127.0.0.1:1080$1 --addr-cli=:637$1 --zone=zone-$1 --rack=rack-$1 --data=/apps/deepfabric/cell$1/data --interval-heartbeat-store=5 --interval-heartbeat-cell=2 &
}

start_elasticell_proxy() {
    $ELASTICELL_DIR/redis-proxy --cfg=$CFG_DIR/proxy.json
}

echo "begin to start pd cluster"
start_elasticell_pd 1
sleep 1
start_elasticell_pd 2
start_elasticell_pd 3
echo "pd cluster is started"

echo "begin to start cell cluster"
start_elasticell_cell 1
sleep 1
start_elasticell_cell 2
start_elasticell_cell 3
echo "cell cluster is started"

sleep 5
echo "begin to start redis proxy"
start_elasticell_proxy
