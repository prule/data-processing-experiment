package com.example.dataprocessingexperiment.tables.statistics

import kotlinx.serialization.Serializable

@Serializable
data class StatisticDefinition(
    val id: String,
    val column: String? = ""
)