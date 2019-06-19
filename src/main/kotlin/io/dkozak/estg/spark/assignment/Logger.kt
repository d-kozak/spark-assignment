package io.dkozak.estg.spark.assignment

import java.io.BufferedWriter
import java.util.*

class Logger(
    private val writer: BufferedWriter
) {

    private val nameStack = LinkedList<String>()

    fun task(name: String, block: Logger.() -> Unit) {
        newLine()
        log("# Starting task $name")
        nameStack.add(name)
        try {
            block()
        } finally {
            nameStack.removeLast()
            log("# Finishing task $name")
            newLine()
        }
    }

    private fun newLine() {
        writer.write("\n")
        println()
    }

    fun log(message: String) {
        val indent = nameStack.map { '\t' }.joinToString(separator = "")
        writer.write("$indent$message\n")
        println("$indent$message")
    }

}