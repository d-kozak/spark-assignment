package io.dkozak.estg.spark.assignment.tasks


import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.`when`
import org.apache.spark.sql.functions.monotonically_increasing_id

val lookupCollection: TaskCode = { dataset, outputDir, _, logger ->
    val companies = prepareCompanyDataset(dataset, outputDir)
        .collectAsList()
        .map { it.getString(0) }
        .mapIndexed { i, company ->
            company to i
        }.toMap()

    logger.log("Companies: $companies")

    val companyColumn = dataset.col("company")
    var whenColumn: Column? = null
    for ((company, id) in companies) {
        whenColumn = if (whenColumn == null) {
            `when`(companyColumn.equalTo(company), id)
        } else {
            whenColumn.`when`(companyColumn.equalTo(company), id)
        }
    }

    dataset.withColumn("company", whenColumn)
        .writeCsv("$outputDir/reviews")
}

private fun prepareCompanyDataset(
    dataset: Dataset<Row>,
    outputDir: String
): Dataset<Row> {
    val companyDataset = dataset.select("company")
        .distinct()
        .coalesce(1)
    companyDataset
        .withColumn("id", monotonically_increasing_id())
        .writeCsv("$outputDir/lookup")
    return companyDataset
}