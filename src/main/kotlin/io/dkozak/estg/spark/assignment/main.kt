package io.dkozak.estg.spark.assignment

import io.dkozak.estg.spark.assignment.tasks.lookupCollection
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.BufferedWriter
import java.io.File


const val LOG_FILE_NAME = "log"

typealias AssignmentTask = (dataset: Dataset<Row>, outputDir: String, log: (String) -> Unit) -> Unit

val allTasks = listOf(lookupCollection)

fun handleArguments(args: Array<String>): Triple<String, String, List<AssignmentTask>> {
    fun fail(message: String): Nothing = throw IllegalArgumentException(message)

    args.size < 2 || args.size > 3 && fail("Args: input_csv_file output_directory [task_number]")
    val inputFile = File(args[0])
    inputFile.exists() || fail("File ${args[0]} does not exist")
    val outputDir = File(args[1])
    if (outputDir.exists()) {
        outputDir.isDirectory || fail("${args[1]} exists and is not a directory")
    } else {
        outputDir.mkdir() || fail("Could not create the output directory")
    }
    return if (args.size == 2) Triple(args[0], args[1], allTasks)
    else {
        val task = args[2].toIntOrNull() ?: fail("${args[2]} is not an integer")
        (task < 1 || task > 12) && fail("$task should be between 1 and 12")
        Triple(args[0], args[1], allTasks.subList(task - 1, task))
    }
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

fun prepareOutput(outputDir: String, block: (BufferedWriter) -> Unit) =
    File("$outputDir/$LOG_FILE_NAME").bufferedWriter().use(block)


fun main(args: Array<String>) {
    val (inputFile, outputDir, tasks) = handleArguments(args)
    sparkExecute { spark ->
        prepareOutput(outputDir) { logger ->
            val dataset = spark.loadCsv(inputFile)
            for (task in tasks) {
                task(dataset, outputDir, logger::println)
            }
        }

    }
}

fun SparkSession.loadCsv(
    inputFile: String,
    header: Boolean = true
): Dataset<Row> {
    return this.read()
        .format("csv")
        .option("header", header)
        .load(inputFile)
        .cache()
}