#!/bin/bash

JOPTS="-XX:+AggressiveOpts -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -Xms512m -Xmx1g"
ROOT=$(readlink -f $(dirname $0)/..)
CP="-cp $ROOT/conf/:$ROOT/*:$ROOT/:$ROOT/lib/*:$ROOT/lib/"
MAIN="net.pixelcop.sewer.Benchmark"
mkdir -p $ROOT/var/

# load hadoop native libs, if avail
NATIVE=`find /usr/lib /usr/local/lib -name 'libhadoop.so*' | head -n 1`
if [ "$NATIVE" != "" ]; then
  export LD_LIBRARY_PATH=`dirname $NATIVE`
fi

RUN="java -Dlog.root=$ROOT/var $JOPTS $CP $MAIN -v"

echo $RUN
echo
echo "starting benchmark..."
time $RUN
