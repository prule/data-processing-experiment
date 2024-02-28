package com.example.dataprocessingexperiment.tables.statistics

import kotlinx.serialization.Serializable

@Serializable
data class StatisticDescriptor(
    val id: String,
    val column: String? = ""
)