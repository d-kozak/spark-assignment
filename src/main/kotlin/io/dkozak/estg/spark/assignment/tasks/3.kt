package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv

val undersampling: TaskCode = { dataset, outputDir, logger ->
    val reviewsPerCompany = dataset.groupBy("company")
        .count()
        .orderBy("count")
        .collectAsList()
        .map { it.getString(0) to it.getLong(1) }

    val (minCompanyName, min) = reviewsPerCompany[0]

    logger.log("Reviews per company $reviewsPerCompany")
    logger.log("Company $minCompanyName has the least reviews: $min")

    val percentagePerCompany = reviewsPerCompany
        .filter { (name, _) -> name != minCompanyName }
        .map { (name, count) -> name to (min.toDouble() / count) }

    logger.log("Will take following percentage of reviews $percentagePerCompany")

    var result = dataset.where(dataset.col("company").equalTo(minCompanyName))
    for ((companyName, percentage) in percentagePerCompany) {
        val sampled = dataset.where(dataset.col("company").equalTo(companyName))
            .sample(percentage)
        result = result.union(sampled)
    }

    logger.log("Final stats: ")
    result.groupBy("company")
        .count()
        .show()

    result.writeCsv("$outputDir/undersampled")

}