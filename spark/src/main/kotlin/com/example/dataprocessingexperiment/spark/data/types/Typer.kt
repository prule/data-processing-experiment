package com.example.dataprocessingexperiment.spark.data.types

import com.example.dataprocessingexperiment.tables.ColumnType
import org.apache.spark.sql.Column

/**
 * For each type conversion supported, there is an implementation of Typer.
 *
 * In the most simple case these can just be a CAST to the required type.
 *
 * A formats parameter can be used to provide information about how to parse the value.
 *
 * DataFrameBuilder will call `process` when building the typed dataframe.
 */
interface Typer: ColumnType {
    fun key(): String
    fun process(name: String): Column
}