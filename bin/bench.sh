#!/bin/bash

JOPTS="-XX:+AggressiveOpts -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -Xms512m -Xmx1g"
ROOT=$(readlink -f $(dirname $0)/..)
CP="-cp $ROOT/conf/:$ROOT/*:$ROOT/:$ROOT/lib/*:$ROOT/lib/"
MAIN="net.pixelcop.sewer.Benchmark"
mkdir -p $ROOT/var/

RUN="java -Dlog.root=$ROOT/var $JOPTS $CP $MAIN -v"

echo $RUN
echo
echo "starting benchmark..."
time $RUN
