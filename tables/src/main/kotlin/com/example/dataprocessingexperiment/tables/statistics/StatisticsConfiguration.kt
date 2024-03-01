package com.example.dataprocessingexperiment.tables.statistics

import com.example.dataprocessingexperiment.tables.FileSource
import kotlinx.serialization.Serializable

@Serializable
class StatisticsConfiguration(
    val statistics: List<Statistics>
) {

    fun statisticsById(id: String): Statistics? {
        return statistics.firstOrNull { source -> source.id == id }
    }
}