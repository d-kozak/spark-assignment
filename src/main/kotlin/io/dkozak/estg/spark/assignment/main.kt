package io.dkozak.estg.spark.assignment

import org.apache.spark.sql.SparkSession

fun main() {
    val inputFile = "/home/dkozak/projects/spark-assignment/employee_reviews.csv"
    val spark = SparkSession.builder().appName("test")
        .orCreate
    val dataset = spark.read().textFile(inputFile).cache()

    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ${dataset.first()}")
    val count = dataset.filter { it.length > 3 }
        .count()

    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! $count")
    spark.stop()
}