package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.sql.functions.desc

val oversampling: TaskCode = { dataset, outputDir, logger ->
    val reviewsPerCompany = dataset.groupBy("company")
        .count()
        .orderBy(desc("count"))
        .collectAsList()
        .map { it.getString(0) to it.getLong(1) }

    val (maxCompanyName, max) = reviewsPerCompany[0]

    logger.log("Reviews per company $reviewsPerCompany")
    logger.log("Company $maxCompanyName has the most reviews: $max")

    val reviewsToAdd = reviewsPerCompany
        .filter { (name, _) -> name != maxCompanyName }
        .map { (name, count) -> Triple(name, count, max - count) }
    logger.log("Need to add $reviewsToAdd")


    var result = dataset.where(dataset.col("company").equalTo(maxCompanyName))
    for ((name, count, toAdd) in reviewsToAdd) {
        logger.task("Oversampling on $name") {
            val reviewsForCompany = dataset.where(dataset.col("company").equalTo(name))

            var buffer = reviewsForCompany
            val fullIterations = toAdd / count
            val reminder = toAdd % count
            val percentage = reminder.toDouble() / count
            logger.log("Need $fullIterations full iterations and another $percentage in the last one")
            for (i in 0 until fullIterations) {
                buffer = buffer.union(reviewsForCompany)
            }
            if (reminder > 0) {
                val subset = reviewsForCompany.sample(percentage)
                buffer = buffer.union(subset)
            }

            logger.log("Final review count for $name is ${buffer.count()}")
            result = result.union(buffer)
        }
    }

    logger.log("Final stats: ")
    result.groupBy("company")
        .count()
        .show()

    result.writeCsv("$outputDir/oversampled")
}