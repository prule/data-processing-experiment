package com.example.dataprocessingexperiment.spark.statistics.collectors

data class StatisticItem(
    val key: String,
    val discriminator: String,
    val value: Any
)