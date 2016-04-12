#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SPARK_HOME="/home/riyad/dev-src/spark-1.6.1"

${SPARK_HOME}/bin/spark-submit \
  --class Main \
  --master local[8] \
  ${DIR}/target/scala-2.10/spark-streaming-hashgraph_2.10-1.0.jar
