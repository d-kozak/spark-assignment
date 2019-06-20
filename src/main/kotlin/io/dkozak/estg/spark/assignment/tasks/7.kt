package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.collect_list
import scala.Serializable
import scala.Tuple2

val invertedIndex: TaskCode = { dataset, outputDir, getColIndex, logger ->

    val idIndex = getColIndex("id")
    val summaryIndex = getColIndex("summary")
    val getText = object : FlatMapFunction<Row, Tuple2<String, String>>, Serializable {

        override fun call(it: Row): MutableIterator<Tuple2<String, String>> {
            val id = it.getString(idIndex)
            val text = (it.getString(summaryIndex) ?: "")
                .split(" ")
                .filter { it.isNotEmpty() }
                .map { Tuple2(it, id) }
            return object : MutableIterator<Tuple2<String, String>> {

                var i = 0

                override fun hasNext(): Boolean = i < text.size

                override fun next(): Tuple2<String, String> = text[i++]

                override fun remove() {
                    TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
                }
            }
        }
    }
    val result = dataset.flatMap(getText, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .groupBy("_1")
        .agg(functions.concat_ws(" ", collect_list("_2")))
        .toDF("word", "documents")

    result.show()
    result.writeCsv("$outputDir/inverted_index")
}