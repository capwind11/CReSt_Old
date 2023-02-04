#!/bin/bash

### ### ###  		   ### ### ###

### ### ### INITIALIZATION ### ### ###

### ### ###  		   ### ### ###

### paths configuration ###
#FLINK_BUILD_PATH="/usr/local/Cellar/apache-flink/1.16.0/"
FLINK_BUILD_PATH=/Users/zhangjinyang/experiment/git_repo/ds2/workspace/flink-1.4.1-instrumented/flink-1.4.1/flink-dist/target/flink-1.4.1-bin/flink-1.4.1/

FLINK=$FLINK_BUILD_PATH$"bin/flink"
JAR_PATH="/Users/zhangjinyang/experiment/CReSt/flink-bencnmark/target/flink-bencnmark-1.0-SNAPSHOT.jar"

### dataflow configuration ###
QUERY_CLASS="org.example.flink.${2}"

if [ $1 == "start" ]; then
    ${FLINK_BUILD_PATH}bin/start-cluster.sh
elif [ $1 == "stop" ]; then
  ${FLINK_BUILD_PATH}bin/stop-cluster.sh
else
  $FLINK run -d --class $QUERY_CLASS $JAR_PATH --mode offline
#  --cfg config/StatefulWordCount.yaml
fi
