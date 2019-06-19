package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import org.apache.spark.sql.functions.`when`

val discretizing: TaskCode = { dataset, outputDir, logger ->

    val col = dataset.col("overall-ratings")
    val columnRule = `when`(col.leq(1), 1)
        .`when`(col.leq(2), 2)
        .`when`(col.leq(3), 3)
        .`when`(col.leq(14), 4)
        .otherwise(5)

    dataset.withColumn("overall-ratings", columnRule)
        .show()
}