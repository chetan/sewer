
# start server
start () {

  echo -n "starting sewer... "

  mkdir -p $VAR

  # check if already running
  if [ -f $PID_FILE ]; then
    PID=`cat $PID_FILE`
    if [ `ps -p $PID` ]; then
      echo "error: already running at pid $PID!"
      exit 1
    fi
  fi

  if [ -z "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME not set"
    exit 1
  fi

  # setup classpath
  . $ROOT/bin/_cp.sh

  RUN="jsvc"
  if [ -n "$USER" ]; then
    RUN="$RUN -user $USER"
  fi

  RUN="$RUN -procname sewer -java-home $JAVA_HOME -pidfile $PID_FILE -errfile &1 -outfile $OUT_FILE -Dlog.root=$VAR $JOPTS $CP $MAIN -v"

  # start
  $RUN

  echo "done"
}

# stop server
stop () {
  echo -n "stopping sewer... "

  jsvc -stop -pidfile $PID_FILE $MAIN

  echo "done"

}
