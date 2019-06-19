package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.AssignmentTask
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.`when`

val lookupCollection: AssignmentTask = { dataset, outputDir, log ->
    val companyDataset = dataset.select("company")
        .distinct()
        .coalesce(1)
    companyDataset.write()
        .option("header", true)
        .csv("$outputDir/lookup")

    val companies = companyDataset
        .collectAsList()

        .map { it.getString(0) }
        .mapIndexed { i, company ->
            company to i
        }.toMap()

    val companyColumn = dataset.col("company")
    var whenColumn: Column? = null
    for ((company, id) in companies) {
        if (whenColumn == null) {
            whenColumn = `when`(companyColumn.equalTo(company), id)
        } else {
            whenColumn = whenColumn.`when`(companyColumn.equalTo(company), id)
        }
    }
    whenColumn!!.otherwise("fooo")

    dataset.withColumn(
        "company", whenColumn
    ).coalesce(1)
        .write()
        .option("header", true).csv("$outputDir/reviews")
}