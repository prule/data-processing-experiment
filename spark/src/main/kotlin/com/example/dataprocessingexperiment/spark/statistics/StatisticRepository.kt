package com.example.dataprocessingexperiment.spark.statistics

/**
 * A registry for supported statistics.
 *
 * If a new statistic is implemented it should be registered via the `add()` method.
 *
 * val statisticsRepository = StatisticRepository()
 * statisticsRepository.add(Count(), "count")
 *
 * val statistics = StatisticRepository().buildStatistics(statisticsConfiguration) // transform from configuration to implementation
 */
class StatisticRepository {
    private val registry: MutableMap<String, Statistic> = mutableMapOf()

    init {
        add(Bounds(""))
        add(Bounds(""), "bounds")
        add(Count(), "count")
        add(CountByValue(""), "countByValue")
        add(CountByMonth(""), "countByMonth")
        add(Maximum(""), "maximum")
        add(Minimum(""), "minimum")
        add(DuplicateCount(), "duplicateCount")
    }

    fun add(statistic: Statistic, alias: String? = null) {
        registry[statistic.javaClass.name] = statistic
        if (alias != null) {
            registry[alias] = statistic
        }
    }

    fun buildStatistics(statistics: com.example.dataprocessingexperiment.tables.statistics.Statistics): List<Statistic> {
        return statistics.values.mapNotNull { insightDescriptor -> registry[insightDescriptor.id]?.of(insightDescriptor.column!!) }
    }
}