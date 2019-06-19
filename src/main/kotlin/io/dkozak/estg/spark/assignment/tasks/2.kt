package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import org.apache.spark.sql.functions.desc

val oversampling: TaskCode = { dataset, outputDir, logger ->
    val reviewsPerCompany = dataset.groupBy("company")
        .count()
        .orderBy(desc("count"))
        .collectAsList()
        .map { it.getString(0) to it.getLong(1) }

    val (companyName, count) = reviewsPerCompany[0]

    logger.log("Reviews per company $reviewsPerCompany")
    logger.log("Company $companyName has the most reviews: $count")
}