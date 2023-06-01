#!/bin/bash

if [ "$1" = "-p" ]; then
    pid_file=$2
    shift 2
fi

# Read in default settings
[ -f /etc/default/vantiq ] && . /etc/default/vantiq

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SERVICE_CONNECTOR_OPTS="$SERVICE_CONNECTOR_OPTS -Dio.vantiq.configDir=$SCRIPT_DIR/../config -Dlogback.configurationFile=$SCRIPT_DIR/../config/logback.xml"

if [ -n "$pid_file" ]; then
    PID=`$SCRIPT_DIR/service-connector "$@" >/dev/null 2>&1 & echo $!`
    echo $PID >> $pid_file
else
    $SCRIPT_DIR/service-connector "$@"
fi