package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.sql.functions.`when`

val discretizing: TaskCode = { dataset, outputDir, _, logger ->

    val col = dataset.col("overall-ratings")
    val columnRule = `when`(col.leq(1), 1)
        .`when`(col.leq(2), 2)
        .`when`(col.leq(3), 3)
        .`when`(col.leq(14), 4)
        .otherwise(5)

    val discretized = dataset.withColumn("overall-ratings", columnRule)
    discretized
        .show()
    discretized.writeCsv("$outputDir/discretized")
}