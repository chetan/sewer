#!/bin/bash

# configurable settings:

# uncomment to override ENV
#JAVA_HOME=

# set USER var to drop privileges to this account (only if jsvc 1.0.8 is avail)
USER=

# Jolokia/JMX agent port
JMX_PORT=7777

ROOT=$(readlink -f $(dirname $0)/..)
TMP=$ROOT/tmp
VAR=$ROOT/var
PID_FILE=$VAR/sewer.pid
OUT_FILE=$VAR/sewer.out

# Java optimizations - these should be good defaults
JOPTS="-XX:+AggressiveOpts -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -Xms512m -Xmx1g"

# AggressiveOpts seems to be pretty good under OpenJDK 7 at least

# might be good for jetty headers?
# -XX:+UseCompressedStrings
# -XX:+UseStringCache
# -XX:+OptimizeStringConcat

# -XX:+DoEscapeAnalysis

# -XX:+AggressiveOpts =

# -XX:+EliminateAutoBox
# -XX:AutoBoxCacheMax=20000
# -XX:BiasedLockingStartupDelay=500
# -XX:+DoEscapeAnalysis
# -XX:+OptimizeStringConcat
# -XX:+OptimizeFill

# this loads the Jolokia JMX agent on port $JMX_PORT
JOPTS="-javaagent:$ROOT/lib/jolokia-jvm-agent-1.0.2.jar=port=$JMX_PORT,host=localhost $JOPTS"

# -----------------------------------------------------------------------------
# shouldn't need to edit below this line


MAIN="net.pixelcop.sewer.node.NodeDaemon"

# load hadoop native libs, if avail
NATIVE=`find /usr/lib /usr/local/lib -name 'libhadoop.so*' | head -n 1`
if [ "$NATIVE" != "" ]; then
  export LD_LIBRARY_PATH=`dirname $NATIVE`
fi

if [ `which jsvc` ]; then
  . $ROOT/bin/_jsvc.sh
else
  . $ROOT/bin/_java.sh
fi

# process command

CMD=$1
if [ "$CMD" == "start" ]; then
  start

elif [ "$CMD" == "stop" ]; then
  stop

else
  echo "usage: $0 <start|stop>"
  exit -1

fi
