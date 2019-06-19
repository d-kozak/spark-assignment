package io.dkozak.estg.spark.assignment

import java.io.BufferedWriter
import java.util.*

class Logger(
    private val writer: BufferedWriter
) {

    private val sectionStack = LinkedList<String>()

    fun section(name: String, block: Logger.() -> Unit) {
        log("# Starting section $name")
        try {
            block()
        } finally {
            log("# Finishing section $name\n")
        }
    }

    fun log(message: String) = writer.write("$message\n")

}