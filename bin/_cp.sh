
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
