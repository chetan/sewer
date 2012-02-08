#!/bin/bash

ROOT=$(readlink -f $(dirname $0)/..)
TMP=$ROOT/tmp
VAR=$ROOT/var
PID_FILE=$VAR/sewer.pid
OUT_FILE=$VAR/sewer.out

# set USER var to drop privileges to this account (only if jsvc is avail)
USER=

JOPTS="-XX:+AggressiveOpts -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -Xms512m -Xmx1g"
JOPTS="-javaagent:$ROOT/lib/jolokia-jvm-agent-1.0.2.jar=port=7777,host=localhost $JOPTS"

# AggressiveOpts seems to be pretty good under OpenJDK 7

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

MAIN="net.pixelcop.sewer.node.Node"

# load hadoop native libs, if avail
NATIVE=`find /usr/lib /usr/local/lib -name 'libhadoop.so*' | head -n 1`
if [ "$NATIVE" != "" ]; then
  export LD_LIBRARY_PATH=`dirname $NATIVE`
fi

# start server
start () {

  echo -n "starting sewer... "

  # check if already running
  if [ -f $PID_FILE ]; then
    PID=`cat $PID_FILE`
    if [ `ps auxw | grep -v grep | grep $PID | wc -l` != 0 ]; then
      echo "error: already running at pid $PID!"
      exit 1
    fi
  fi

  # setup classpath
  if [ -d $ROOT/target ]; then
    # in dev env
    echo "testing"

  else
    # dist path
    CP=`ls $ROOT/*.jar | grep -v sources`
    CP="$CP:$ROOT/lib/*:$ROOT/lib/"

  fi

  if [ -d $ROOT/conf ]; then
    CP="$ROOT/conf:$CP"
  fi
  CP="-cp $CP"

  if [ `which jsvc` ]; then
    RUN="jsvc"
    if [ -n "$USER" ]; then
      RUN="$RUN --user $USER"
    fi

  else
    RUN="java"
  fi

  RUN="$RUN -Dlog.root=$VAR $JOPTS $CP $MAIN -v"

  # start
  mkdir -p $VAR
  $RUN 2>>$OUT_FILE >>$OUT_FILE &

  # write pid
  PID=$!
  rm -f $PID_FILE
  echo $PID > $PID_FILE
  echo "done"

}

# stop server
stop () {
  echo -n "stopping sewer... "

  if [ -f $PID_FILE ]; then
    PID=`cat $PID_FILE`
    if [ `ps auxw | grep -v grep | grep $PID | wc -l` != 0 ]; then
      kill $PID
      echo "done"

    else
      echo "pid $PID not found. died?"
    fi

    rm -f $PID_FILE

  else
    echo "not running!"
  fi

}

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
