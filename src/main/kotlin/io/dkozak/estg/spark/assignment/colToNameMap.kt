package io.dkozak.estg.spark.assignment

import java.io.File

fun loadColIndexes(inputCsv: String): (String) -> Int {
    val header = File(inputCsv).bufferedReader().use { it.readLine() }
    val map = header.split(",")
        .mapIndexed { index, name -> name to index }
        .toMap()
    return { name -> map[name] ?: throw IllegalArgumentException("Unknown name $name") }
}