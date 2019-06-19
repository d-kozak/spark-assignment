package io.dkozak.estg.spark.assignment

import io.dkozak.estg.spark.assignment.tasks.lookupCollection
import io.dkozak.estg.spark.assignment.tasks.oversampling
import io.dkozak.estg.spark.assignment.tasks.undersampling
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.BufferedWriter
import java.io.File


const val LOG_FILE_NAME = "log"


val allTasks = tasks(
    Task(1, "Lookup Collection", lookupCollection),
    Task(2, "Oversampling", oversampling),
    Task(3, "Undersampling", undersampling)
)


fun handleArguments(args: Array<String>): Triple<String, String, Map<Int, Task>> {
    fun fail(message: String): Nothing = throw IllegalArgumentException(message)
    (args.size < 2 || args.size > 3) && fail("Expecting arguments: input_csv_file output_directory [task_number]")
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
        val taskId = args[2].toIntOrNull() ?: fail("${args[2]} is not an integer")
        val task = allTasks[taskId] ?: fail("Task with id $taskId not found")
        Triple(args[0], args[1], mapOf(taskId to task))
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

fun prepareOutput(outputDir: String, block: (BufferedWriter) -> Unit) =
    File("$outputDir/$LOG_FILE_NAME").bufferedWriter().use(block)

fun Dataset<*>.writeCsv(name: String) = this.coalesce(1)
    .write()
    .option("header", true)
    .csv(name)

fun main(args: Array<String>) {
    val (inputFile, outputDir, tasks) = handleArguments(args)
    sparkExecute { spark ->
        prepareOutput(outputDir) { writer ->
            val logger = Logger(writer)
            val dataset = spark.loadCsv(inputFile)
            for ((index, task) in tasks) {
                val taskOutputDir = "$outputDir/$index"
                File(taskOutputDir).mkdir() || throw RuntimeException("Could not create output dir $taskOutputDir")
                logger.task(task.name) {
                    task.code(dataset, taskOutputDir, logger)
                }
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