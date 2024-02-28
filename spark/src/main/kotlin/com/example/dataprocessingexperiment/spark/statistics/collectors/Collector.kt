package com.example.dataprocessingexperiment.spark.statistics.collectors

import java.io.Closeable

interface Collector: Closeable {
    fun add(key: String, column: String, discriminator: String, value: Any)
}