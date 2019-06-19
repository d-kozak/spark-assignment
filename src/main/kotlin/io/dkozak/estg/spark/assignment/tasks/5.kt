package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import java.io.File

val probabilistic: TaskCode = { dataset, outputDir, logger ->
    val totalReviews = dataset.count().toDouble()
    val probabilities = dataset.groupBy("company")
        .count()
        .collectAsList()
        .map { it.getString(0) to it.getLong(1) }
        .map { (name, count) -> name to count / totalReviews }

    logger.log("Probabilities per company are $probabilities")

    File("$outputDir/probabilities.csv").bufferedWriter().use {
        it.write("company,probability\n")
        for ((name, probability) in probabilities) {
            it.write("$name,$probability\n")
        }
    }
}