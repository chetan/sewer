
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

  # setup classpath
  . $ROOT/bin/_cp.sh

  RUN="java -Dlog.root=$VAR $JOPTS $CP $MAIN -v"

  # start
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
