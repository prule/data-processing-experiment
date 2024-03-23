package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.data.SparkDataTypes
import com.example.dataprocessingexperiment.spark.statistics.collectors.Collector
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import mu.KotlinLogging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions

/**
 * Todo: is there a better way to do this? Current implementation counts nulls for each column one at a time.
 * This wouldn't perform well if data was large. Can it be done without scanning the table for each column?
 */
@Serializable
class NullCount(private val columns: List<String>) : Statistic {
    private val logger = KotlinLogging.logger {}

    @Transient
    private val sparkDataTypes = SparkDataTypes()

    override fun run(data: Dataset<Row>, collector: Collector) {
        val cols = if (columns.isNotEmpty()) {
            data.columns().intersect(columns)
        } else {
            data.columns().toList()
        }

        val fields = data.schema().toList()
        fields.foreach {
            if (cols.contains(it.name())) {
                val type = sparkDataTypes.type(it.dataType())
                if (type != null) {
                    val count = data.where(type.nullPredicate.invoke(functions.col(it.name()), it.nullable())).count()
                    collector.add(
                        "NullCount", it.name(), "", count
                    )
                } else {
                    logger.warn { "Skipping null count for ${it.name()} because type could not be determined" }
                }
            }
        }

    }
}