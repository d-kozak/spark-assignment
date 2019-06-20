package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.sum
import scala.Function1
import scala.Serializable

val tf_idf: TaskCode = { dataset, outputDir, getColIndex, logger ->
    val word = "work"
    val summaryIndex = getColIndex("summary")

    val containsWord = object : Function1<Row, Int>, Serializable {
        override fun apply(it: Row): Int {
            return if ((it.getString(summaryIndex) ?: "").contains(" $word ")) 1 else 0
        }
    }

    val count = dataset.map(containsWord, Encoders.INT())
        .agg(sum("value")).collectAsList()[0].getLong(0).toDouble()

    val idf = Math.log(dataset.count() / count)

    logger.log("The number of documents containing the word '$word' is $count")
    logger.log("Idf is $idf")


    val idfCalc = object : Function1<Row, Double>, Serializable {
        override fun apply(it: Row): Double {
            val words = (it.getString(summaryIndex) ?: "").split(" ")
            val workCount = words.count { it == word }.toDouble()
            val tf = workCount / words.size
            return tf * idf
        }
    }

    val tf_idf = dataset.map(idfCalc, Encoders.DOUBLE())

    tf_idf.show()
    tf_idf.writeCsv("$outputDir/tf_idf")
}