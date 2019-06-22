#!/usr/bin/env bash
if [[ -z ${1} ]]; then
    echo "Path to spark not specified"
    exit 1
fi
SPARK_EXEC=$1/bin/spark-submit
if [[ ! -f ${SPARK_EXEC} ]]; then
    echo "Could not find spark instance at ${SPARK_EXEC}"
    exit 1
fi

JAR_FILE="build/libs/spark-assignment-1.0-SNAPSHOT.jar"
INPUT_FILE="./employee_reviews.csv"
OUTPUT_DIR="./output"

rm -rf ${OUTPUT_DIR}

gradle jar || exit 1

${SPARK_EXEC} --class io.dkozak.estg.spark.assignment.MainKt --master "local[4]" ${JAR_FILE} ${INPUT_FILE} ${OUTPUT_DIR} $2 2>&1 | grep -v "INFO"

