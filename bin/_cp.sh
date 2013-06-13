
# setup classpath
if [ -d $ROOT/target ]; then
  # in dev env
  echo "testing"

else
  # dist path
  CP=`ls $ROOT/*.jar | egrep -v 'sources|tests' | tr "\n" ":"`
  CP="$CP:$ROOT/lib/*:$ROOT/lib/"

fi

if [ -d $ROOT/conf ]; then
  CP="$ROOT/conf:$CP"
fi
CP="-cp $CP"
