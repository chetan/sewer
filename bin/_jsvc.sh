
# start server
start () {

  echo -n "starting sewer... "

  mkdir -p $VAR

  # check if already running
  if [ -f $PID_FILE ]; then
    PID=`cat $PID_FILE`
    if [ `ps auxw | grep -v grep | grep $PID | wc -l` != 0 ]; then
      echo "error: already running at pid $PID!"
      exit 1
    fi
  fi

  if [ -z "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME not set"
    exit 1
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
