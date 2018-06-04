#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
APPLICATION_HOME=$bin
APPLICATION_HOME=`cd "$APPLICATION_HOME">/dev/null; pwd`

for f in $APPLICATION_HOME/lib/*.jar; do
  JARS="$f,${JARS}"
done

echo "$JARS"

SPARK_SUBMIT=/home/oio/app/spark-2.2.0-bin-without-hadoop/bin/spark-submit

JAR="$APPLICATION_HOME/lib/wh-tools-1.0-SNAPSHOT.jar"

CONF="$APPLICATION_HOME/conf/spark-custom.conf"

MAIN_CLASS="cn.gaei.ev.wh.LonLatToCity"

FILES="$APPLICATION_HOME/conf/rel_path_added.txt"

ARGS=

$SPARK_SUBMIT --jars "$JARS" --file $FILES --properties-file $CONF  --class $MAIN_CLASS  $JAR $ARGS
