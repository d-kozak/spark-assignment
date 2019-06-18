package io.dkozak.estg.spark.assignment

import org.apache.spark.sql.SparkSession
import java.io.BufferedWriter
import java.io.File


fun handleArguments(args: Array<String>): Pair<String, String> {
    args.size != 2 && throw IllegalArgumentException("Two arguments expected, input csv file and output file")
    val inputFile = args[0]
    val outputFile = args[1]
    return inputFile to outputFile
}

fun sparkExecute(block: (SparkSession) -> Unit) {
    val spark = SparkSession.builder().appName("App")
        .orCreate
    try {
        block(spark)
    } finally {
        spark.stop()
    }

}

fun BufferedWriter.println(text: String) = this.write("$text\n")

fun fileOutput(path: String, block: (BufferedWriter) -> Unit) = File(path).bufferedWriter().use(block)

fun main(args: Array<String>) {
    val (inputFile, outputFile) = handleArguments(args)
    sparkExecute { spark ->
        fileOutput(outputFile) { output ->
            val dataset = spark.read().textFile(inputFile).cache()
            output.println("First line: ${dataset.first()}")
            val count = dataset.filter { it.length > 3 }
                .count()

            output.println("Line count: $count")
        }
    }

}