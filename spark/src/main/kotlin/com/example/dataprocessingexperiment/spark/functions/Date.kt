package com.example.dataprocessingexperiment.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.*

/**
 * Formats the column using each of the supplied date formats and uses the first non-null value.
 */
class Date(private val formats: List<String>) {

    fun parse(col: Column): Column {
        return coalesce(*formattedColumns(col).map { it }.toTypedArray())
    }

    private fun formattedColumns(col: Column): List<Column> {
        return formats.map { format -> to_date(col, format)}
    }
}