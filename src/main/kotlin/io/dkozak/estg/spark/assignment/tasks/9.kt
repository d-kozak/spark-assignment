package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import scala.Function1
import scala.Serializable


val normalization: TaskCode = { dataset, outputDir, getColIndex, logger ->
    val colIndex = getColIndex("overall-ratings")

    val normalizationFunction = object : Function1<Row, Double>, Serializable {
        override fun apply(it: Row): Double {
            val rating = it.getString(colIndex).toDoubleOrNull()
                ?: throw IllegalArgumentException("Could not parse ${it.getString(colIndex)}")
            return rating / 5
        }
    }

    val resultCol = dataset.map(normalizationFunction, Encoders.DOUBLE())
        .withColumn("id", functions.monotonically_increasing_id())
    resultCol.show()

    val joined = dataset
        .drop("overall-ratings")
        .join(resultCol, "id")
    joined.show()
    joined.writeCsv("$outputDir/normalized", false)
}