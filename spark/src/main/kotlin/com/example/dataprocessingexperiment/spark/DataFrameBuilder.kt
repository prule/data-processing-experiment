package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.spark.types.Types
import com.example.dataprocessingexperiment.tables.FileSource
import org.apache.spark.sql.*

class DataFrameBuilder(private val sparkSession: SparkSession, private val fileSource: FileSource, private val types: Types, private val rootPath: String = "") {

    // The raw dataset - all columns
    val raw: Dataset<Row> by lazy {
        sparkSession.read()
            .format(fileSource.type)
            .option("header", true)
            .load(rootPath + fileSource.path)
            .alias(fileSource.name)
    }

    // Builds a typed dataset using the definition in the table configuration
    // - Only the columns specified and with their associated types (integer, date, boolean etc)
    // - Those values that couldn't be converted to a type will be null.
    fun typed(): Dataset<Row> {
        val typedColumns: List<Column> = fileSource.table.columns.map { column -> typeColumn(column.name, column.type, column.formats) }
        // call var args function https://stackoverflow.com/a/65520425
        return raw.select(*typedColumns.map { it }.toTypedArray())
    }

    private fun typeColumn(name: String, type: String, formats: List<String>?): Column {
        return types.get(type).process(name, type, formats)
    }

}