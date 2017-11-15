#!/bin/bash
# http://blog.scphillips.com/posts/2013/07/getting-a-python-script-to-run-in-the-background-as-a-service-on-boot/
# https://stackoverflow.com/questions/8251933/how-can-i-log-the-stdout-of-a-process-started-by-start-stop-daemon

ELASTICELL_SRC_DIR=$HOME/src/github.com/deepfabric/elasticell
ELASTICELL_DIR=/apps/deepfabric
CFG_DIR=$ELASTICELL_DIR/cfg
LOG_DIR=$ELASTICELL_DIR/log

start_elasticell_pd() {
    start-stop-daemon --start --oknodo --background --make-pidfile --pidfile $ELASTICELL_DIR/pd$1.pid --startas $ELASTICELL_DIR/pd -- --cfg=$CFG_DIR/pd$1.json --log-file=$LOG_DIR/pd$1.log --log-level=debug
}

start_elasticell_cell() {
    start-stop-daemon --start --oknodo --background --make-pidfile --pidfile $ELASTICELL_DIR/cell$1.pid --startas $ELASTICELL_DIR/cell -- --pd=127.0.0.1:20801,127.0.0.1:20802,127.0.0.1:20803 --addr=127.0.0.1:1080$1 --addr-cli=:637$1 --zone=zone-$1 --rack=rack-$1 --data=$ELASTICELL_DIR/cell$1/data --interval-heartbeat-store=5 --interval-heartbeat-cell=2 --addr-pprof=:638$1 --log-file=$LOG_DIR/cell$1.log --log-level=debug
}

start_elasticell_proxy() {
#    start-stop-daemon --start --oknodo --background --make-pidfile --pidfile $ELASTICELL_DIR/proxy.pid --startas /bin/bash -- -c "exec $ELASTICELL_DIR/redis-proxy --cfg=$CFG_DIR/proxy.json --log-level=debug > $LOG_DIR/proxy.log 2>&1"
    start-stop-daemon --start --oknodo --background --make-pidfile --pidfile $ELASTICELL_DIR/proxy.pid --startas $ELASTICELL_DIR/redis-proxy -- --cfg=$CFG_DIR/proxy.json --log-file=$LOG_DIR/proxy.log --log-level=debug
}

stop_elasticell() {
    start-stop-daemon --stop --oknodo --quiet --retry TERM/5/forever/KILL/1 --remove-pidfile --pidfile $ELASTICELL_DIR/$1$2.pid
#    start-stop-daemon --stop --oknodo --signal KILL --remove-pidfile --pidfile $ELASTICELL_DIR/$1$2.pid
}

status_elasticell() {
    start-stop-daemon --status --pidfile $ELASTICELL_DIR/$1$2.pid
    print_status $1$2 $?
}

print_status() {
    case $2 in
	0)
	    echo "$1: Program is running."
	    ;;
        1)
	    echo "$1: Program is not running and the pid file exists."
	    ;;
	3)
	    echo "$1: Program is not running."
	    ;;
	4)
	    echo "$1: Program is not running."
	    ;;
	*)
	    echo "$1: Program status in unknown."
	    ;;
    esac
}

do_start () {
    do_start_pd
    do_start_cell
    do_start_proxy

    sleep 3
    echo "--------------------"
    do_status
}

do_stop () {
    do_stop_proxy
    do_stop_cell
    do_stop_pd

    echo "--------------------"
    do_status
}


do_start_pd () {
    export GOTRACEBACK=crash

    rm -rf $LOG_DIR/pd*.log*
    echo "staring pd cluster"
    start_elasticell_pd 1
    start_elasticell_pd 2
    start_elasticell_pd 3
    sleep 3
    echo "started pd cluster"    
}

do_stop_pd () {
    echo "stopping pd cluster"
    echo "stopping pd 3"
    stop_elasticell pd 3
    echo "stopping pd 2"
    stop_elasticell pd 2
    echo "stopping pd 1"
    stop_elasticell pd 1
    echo "stopped pd cluster"
}

do_start_cell () {
    export GOTRACEBACK=crash

    rm -rf $LOG_DIR/cell*.log*
    echo "staring cell cluster"
    start_elasticell_cell 1
    sleep 2
    start_elasticell_cell 2
    start_elasticell_cell 3
    echo "started cell cluster"
}

do_stop_cell () {
    echo "stopping cell cluster"
    echo "stopping cell 3"
    stop_elasticell cell 3
    echo "stopping cell 2"
    stop_elasticell cell 2
    echo "stopping cell 1"
    stop_elasticell cell 1
    echo "stopped cell cluster"
}

do_start_proxy () {
    rm -rf $LOG_DIR/proxy*.log*
    echo "staring redis proxy"
    start_elasticell_proxy
    echo "started redis proxy"
}

do_stop_proxy () {
    echo "stopping redis proxy"
    stop_elasticell proxy
    echo "stopped redis proxy"
}

do_status () {
    status_elasticell pd 1
    status_elasticell pd 2
    status_elasticell pd 3
    status_elasticell cell 1
    status_elasticell cell 2
    status_elasticell cell 3
    status_elasticell proxy
}

do_clear () {
    echo "clearing date, index and log......"
    mkdir -p $CFG_DIR $LOG_DIR &&
    cd $ELASTICELL_DIR &&
    rm -rf pd1/* pd2/* pd3/* cell1/* cell2/* cell3/* log/* &&
    mkdir -p pd1/data pd2/data pd3/data &&
    mkdir -p cell1/data cell1/index &&
    mkdir -p cell2/data cell2/index &&
    mkdir -p cell3/data cell3/index
}

do_build () {
    mkdir -p $CFG_DIR $LOG_DIR &&
    cp $ELASTICELL_SRC_DIR/quickstart-cfgs/* $CFG_DIR &&
    cd $ELASTICELL_SRC_DIR/pkg/pb &&
    echo "compiling *.pb......" && bash gen.sh > /dev/null &&
    cd $ELASTICELL_SRC_DIR &&
    echo "building pd......" && go build -gcflags "-N -l"  -o $ELASTICELL_DIR/pd github.com/deepfabric/elasticell/cmd/pd &&
    echo "building cell......" && go build -gcflags "-N -l" -o $ELASTICELL_DIR/cell github.com/deepfabric/elasticell/cmd/cell &&
    echo "building redis-proxy......" && go build -gcflags "-N -l" -o $ELASTICELL_DIR/redis-proxy github.com/deepfabric/elasticell-proxy/cmd/redis &&
    echo "building query......" && go build -gcflags "-N -l" -o $ELASTICELL_DIR/query github.com/deepfabric/elasticell/cmd/query &&
    echo "building fill......" && go build -gcflags "-N -l" -o $ELASTICELL_DIR/fill github.com/deepfabric/elasticell/cmd/fill
}

case "$1" in
    start|stop|status|clear|start_pd|stop_pd|start_cell|stop_cell|start_proxy|stop_proxy|build)
        do_${1}
        ;;
    restart|reload|force-reload)
        do_stop && do_start
        ;;
    update)
        do_stop && do_clear && do_build && do_start
        ;;
    update2)
        do_stop && do_build && do_start
        ;;
    *)
        echo "Usage: cluster.sh {start|stop|restart|status|clear|start_pd|stop_pd|start_cell|stop_cell|start_proxy|stop_proxy|build|update|update2}"
        exit 1
        ;;
esac

exit 0
