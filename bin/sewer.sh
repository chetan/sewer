#!/bin/bash

ROOT=$(dirname $(readlink -f $0))
ROOT=`readlink -f $ROOT/..`
TMP=$ROOT/tmp
PID_FILE=$TMP/sewer.pid
OUT_FILE=$TMP/sewer.out

RUN="java"
RUN="$RUN -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -Xms512m -Xmx1g"

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
    JARS=""
    for jar in `find $ROOT -name '*.jar' | sort -r`; do
      if [ "$JARS" != "" ]; then
        JARS="$JARS:"
      fi
      JARS="$JARS$jar"
    done
    RUN="$RUN -classpath $JARS"
    RUN="$RUN $MAIN"
  fi

  if [ -f $ROOT/conf/config.properties ]; then
    RUN="$RUN -c $ROOT/conf/config.properties"
  fi

  RUN="$RUN -v"

  # start
  mkdir -p $TMP
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
    "not running!"
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
