package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv

import org.apache.spark.sql.functions.avg

val pivotTable: TaskCode = { dataset, outputDir, getColIndex, logger ->
    val pivot = dataset
        .groupBy("company")
        .agg(
            avg("overall-ratings"),
            avg("work-balance-stars"),
            avg("culture-values-stars"),
            avg("carrer-opportunities-stars")
        )
    pivot.show()
    pivot.writeCsv("$outputDir/pivot")
}