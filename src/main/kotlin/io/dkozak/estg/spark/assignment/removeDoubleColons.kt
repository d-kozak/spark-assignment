package io.dkozak.estg.spark.assignment

import java.io.File

fun main() {
    File("employee_reviews.csv").bufferedReader().useLines { lines ->
        File("update.csv").bufferedWriter().use { writer ->
            for (line in lines) {
                writer.write(line.replace("\"\"", "") + "\n")
            }
        }
    }

}
