package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.spark.types.StringType
import com.example.dataprocessingexperiment.spark.types.Types
import com.example.dataprocessingexperiment.tables.FileSource
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim

/**
 * DataFrameBuilder reads the RAW dataset and uses the FileSource configuration to produce a TYPED dataset. The typed dataset
 * will only contain the columns specified in the configuration and uses the Types to do the conversion.
 */
class DataFrameBuilder(
    private val sparkSession: SparkSession,
    private val fileSource: FileSource,
    private val types: Types,
    private val rootPath: String = ""
) {

    private val string = StringType()

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
        val typedColumns: List<Column> =
            fileSource.table.columns.map { column -> types.get(column.type).process(column.name, column.formats) }
        // call var args function https://stackoverflow.com/a/65520425
        return raw.select(*typedColumns.map { it }.toTypedArray())
    }

    fun valid(): Dataset<Row> {
        // required columns != null conditions
        val requiredColumns: List<Column> = fileSource.table.columns
            .filter { column -> column.required }
            .map { column ->
                // for strings, check for null and empty strings
                if (string.key() == column.type)
                    col(column.name).isNotNull.and(
                        trim(col(column.name)).notEqual(functions.lit(""))
                    )
                else
                // otherwise just check for null
                    col(column.name).isNotNull
            }
        // and all columns together so none of the required columns can be null
        var combined: Column? = null
        requiredColumns.forEach { col ->
            combined = if (combined == null) col else combined!!.and(col)
        }
        // select all columns where the required columns are not null
        return typed().select("*").where(combined)
    }

}