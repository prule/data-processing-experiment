package com.example.dataprocessingexperiment.tables.statistics

import kotlinx.serialization.Serializable

@Serializable
class Statistics(
    val id: String,
    val name: String,
    val description: String,
    val values: List<StatisticDefinition>
)