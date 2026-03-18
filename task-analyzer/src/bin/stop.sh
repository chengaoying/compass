#!/bin/bash

HOME_DIR=$(cd $(dirname $0)/.. && pwd)
PID_FILE=${HOME_DIR}/tpid

if [ -f ${PID_FILE} ]; then
  pid=$(cat ${PID_FILE})
  if kill -0 ${pid} 2>/dev/null; then
    kill ${pid}
    echo "task-analyzer stopped (pid=${pid})"
  else
    echo "task-analyzer is not running"
  fi
  rm -f ${PID_FILE}
else
  echo "PID file not found"
fi
