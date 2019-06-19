package io.dkozak.estg.spark.assignment

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

fun tasks(vararg tasks: Task): Map<Int, Task> = tasks.map { it.id to it }.toMap()

data class Task(
    val id: Int,
    val name: String,
    val code: TaskCode
)

typealias TaskCode = (dataset: Dataset<Row>, outputDir: String, logger: Logger) -> Unit