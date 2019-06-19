package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import org.apache.spark.sql.functions.desc

val oversampling: TaskCode = { dataset, outputDir, logger ->
    val companyCount = dataset.groupBy("company")
        .count()
    companyCount
        .show()
    val (companyName, count) = companyCount.orderBy(desc("count"))
        .takeAsList(1)
        .map { it.getString(0) to it.getLong(1) }[0]

    logger.log("Company $companyName has the most reviews: $count")
}