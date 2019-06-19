package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*

val removeNoise: TaskCode = { dataset, outputDir, getColIndex, logger ->

    val dateColIndex = getColIndex("dates")
    val dateFormat = SimpleDateFormat("MMM dd, yyyy")

    val limit = Date(2010, 1, 1)

    val filtered = dataset.filter {
        val dateString = it.getString(dateColIndex).trim()
        try {
            val date = dateFormat.parse(dateString)
            val result = date.after(limit) || date >= limit || date.toInstant() >= limit.toInstant()
            if (date.year > 2010)
                println("'$dateString','$date', $result")
            result

        } catch (ex: ParseException) {
            false
        }
    }

    filtered.show()
    filtered.writeCsv("$outputDir/withoutNoise")
}