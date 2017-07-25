#!/bin/bash

HOME=$ELASTICELL_HOME
EXEC=$HOME/$ELASTICELL_EXEC

LOG_LEVEL=$ELASTICELL_LOG_LEVEL
LOG_TARGET=$ELASTICELL_LOG_TARGET
CFG_FILE=$HOME/cfg/cfg.json
LOG_FILE=$HOME/log/$ELASTICELL_EXEC.log

if [ "$LOG_TARGET" = "CONSOLE" ]; then
    $EXEC --cfg=$CFG_FILE --log-level=$LOG_LEVEL
elif [ "$LOG_TARGET" = "FILE" ]; then
    $EXEC --cfg=$CFG_FILE --log-level=$LOG_LEVEL --log-file=$LOG_FILE  
else
    echo "invalid ELASTICELL_LOG_TARGET env: $LOG_TARGET"
fi

