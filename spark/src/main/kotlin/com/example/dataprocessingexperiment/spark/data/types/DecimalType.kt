package com.example.dataprocessingexperiment.spark.data.types

import mu.KotlinLogging
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

/**
 * Converts to a decimal.
 *
 * The following column definition specifies a decimal with precision=10 and scale=2
 *
 * ```
 *       {
 *         name: "amount",
 *         description: "amount can be a positive (credit) or negative (debit) number representing dollars and cents",
 *         type: "decimal",
 *         formats: [
 *           "10",
 *           "2"
 *         ]
 *       }
 * ```
 *
 * @link https://spark.apache.org/docs/3.5.0/api/java/org/apache/spark/sql/types/DecimalType.html
 */
class DecimalType : Typer {
    private val logger = KotlinLogging.logger {}

    override fun key(): String {
        return "decimal"
    }

    override fun process(name: String, formats: List<String>?): Column {
        var typeCast = "decimal"
        if (formats != null) {
            if (formats.isNotEmpty()) {
                val precision = formats[0].toIntOrNull()
                val scale = if (formats.size > 1) formats[1].toIntOrNull() else null
                if (precision != null && scale != null) {
                    typeCast = "decimal(${precision},${scale})"
                    logger.debug { "Using $typeCast for column $name" }
                } else {
                    logger.debug { "Invalid formats provided so using default $typeCast for column $name" }
                }
            } else {
                logger.debug { "No formats provided so using default $typeCast for column $name" }
            }
        }
        return functions.col(name).cast(typeCast).alias(name)
    }
}