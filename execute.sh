#!/usr/bin/env bash
SPARK_BIN="/home/dkozak/spark-2.4.3-bin-hadoop2.7/bin"

JAR_FILE="build/libs/spark-assignment-1.0-SNAPSHOT.jar"

#if [[ ! -f ${JAR_FILE} ]]; then
#    gradle jar || exit 1
#fi

gradle jar && ${SPARK_BIN}/spark-submit --class io.dkozak.estg.spark.assignment.MainKt --master "local[4]" ${JAR_FILE}

