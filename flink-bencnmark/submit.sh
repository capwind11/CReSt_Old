#!/bin/bash
set -x
### paths configuration ###
#FLINK_BUILD_PATH="/usr/local/Cellar/apache-flink/1.16.0/"
FLINK=/Users/zhangyang/opt/flink-1.16.1/bin/flink
JAR_PATH=/Users/zhangyang/experiment/CReSt/flink-bencnmark/target/flink-bencnmark-1.0-SNAPSHOT.jar
### dataflow configuration ###
QUERY_CLASS="org.example.flink.wordcount.StatefulWordCount"
ADDRESS="localhost/30011/30010"

#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode normal -p 5 --source-rate 120000"
$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode cross-region --p1 2 --p2 3 --cfg /Users/zhangyang/experiment/CReSt/flink-bencnmark/config/StatefulWordCount.yaml --source-rate 60000"
#
#
#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode normal -p 2 --source-rate 60000"
#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode cross-region --p1 1 --p2 2 --cfg /Users/zhangyang/experiment/CReSt/flink-bencnmark/config/StatefulWordCount.yaml --source-rate 120000"
#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode cross-region --p1 1 --p2 3 --cfg /Users/zhangyang/experiment/CReSt/flink-bencnmark/config/StatefulWordCount.yaml --source-rate 120000"
#
#
#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode normal -p 4 --source-rate 60000"
#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode cross-region --p1 2 --p2 2 --cfg /Users/zhangyang/experiment/CReSt/flink-bencnmark/config/StatefulWordCount.yaml --source-rate 120000"
#$FLINK run -d --class org.example.flink.common.SubmitTool $JAR_PATH --q $QUERY_CLASS --address $ADDRESS --f $JAR_PATH "--mode cross-region --p1 2 --p2 3 --cfg /Users/zhangyang/experiment/CReSt/flink-bencnmark/config/StatefulWordCount.yaml --source-rate 120000"
