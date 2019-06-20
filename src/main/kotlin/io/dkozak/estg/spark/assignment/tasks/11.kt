package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.sql.functions.`when`
import org.apache.spark.sql.functions.avg

val fillMissing: TaskCode = { dataset, outputDir, getColIndex, logger ->
    val avgRating = dataset.agg(avg("overall-ratings"))
        .collectAsList()[0]
        .getDouble(0)

    logger.log("Average rating is $avgRating")

    val addAverage = `when`(dataset.col("overall-ratings").isNull, avgRating)

    dataset.withColumn("overall-ratings", addAverage)

    dataset.show()
    dataset.writeCsv("$outputDir/fill_missing")
}