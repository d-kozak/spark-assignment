package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv

val crossValidation: TaskCode = { dataset, outputDir, _, logger ->
    val k = 5
    val foldSize = (dataset.count() / k).toInt()
    logger.log("Generating $k disjoint datasets, each will have $foldSize elements")

    for (i in 0 until k) {
        val start = i * foldSize

        val rows = dataset
            .where(dataset.col("id").`$greater$eq`(start))
            .limit(foldSize)

        rows.show()
        rows.writeCsv("$outputDir/fold_$i")
    }
}