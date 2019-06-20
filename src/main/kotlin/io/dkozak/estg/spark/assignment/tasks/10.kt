package io.dkozak.estg.spark.assignment.tasks

import io.dkozak.estg.spark.assignment.TaskCode
import io.dkozak.estg.spark.assignment.writeCsv
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

val removeNoise: TaskCode = { dataset, outputDir, getColIndex, logger ->
    val original = dataset.count()
    val dateColIndex = getColIndex("dates")
    val limit = LocalDate.of(2017, 1, 1)

    val filtered = dataset.filter {
        val dateString = it.getString(dateColIndex).trim()
        try {
            val dateFormat = DateTimeFormatter.ofPattern("MMM d, yyyy")
            val date = LocalDate.parse(dateString, dateFormat)
            date >= limit

        } catch (ex: DateTimeParseException) {
            false
        }
    }
    val difference = original - filtered.count()

    filtered.show()
    filtered.writeCsv("$outputDir/without_noise")

    logger.log("Removed $difference lines")
}