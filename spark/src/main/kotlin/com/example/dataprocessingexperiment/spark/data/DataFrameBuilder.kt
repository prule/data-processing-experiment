package com.example.dataprocessingexperiment.spark.data

import com.example.dataprocessingexperiment.spark.data.types.StringType
import com.example.dataprocessingexperiment.spark.data.types.Types
import com.example.dataprocessingexperiment.tables.FileSource
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim

/**
 * DataFrameBuilder goes through the following steps
 * 1. reads the RAW dataset.
 * 2. creates the SELECTED dataset using only columns specified in configuration and names columns according to aliases.
 * 3. Uses the configuration to produce a TYPED dataset. The typed dataset uses Types to do the conversion.
 * 4. Produces a VALID dataset by filtering rows without required values and de-duplicating.
 */
class DataFrameBuilder(
    private val sparkSession: SparkSession,
    private val fileSource: FileSource,
    private val types: Types,
    private val rootPath: String = ""
) {

    private val string = StringType()

    /**
     * Loads the raw dataset containing all columns as strings.
     */
    val raw: Dataset<Row> by lazy {
        sparkSession.read()
            .format(fileSource.type)
            .option("header", true) // headers are always required at this point
            .option("delimiter", fileSource.table.delimiter)
            .load(rootPath + fileSource.path)
            .alias(fileSource.name)
    }

    /**
     * Selects only the columns specified in configuration and maps them to the alias.
     */
    fun selected(): Dataset<Row> {
        val columns: List<Column> =
            fileSource.table.columns.map { column ->
                var c: Column? = null
                for (name in column.name) {
                    if (raw.columns().contains(name)) {
                        c = col(name)
                        break
                    }
                }
                if (c == null) {
                    throw RuntimeException("Could not find any of the columns ${fileSource.table.columns} on table ${fileSource.id} at ${fileSource.path}")
                }
                // rename column to alias
                c.`as`(column.alias)
            }

        return raw.select(*columns.map { it }.toTypedArray())
    }

    /**
     * Builds a typed dataset using the definition in the table configuration
     *
     * - Only the columns specified and with their associated types (integer, date, boolean etc)
     * - Those values that couldn't be converted to a type will be null.
     *
     * Also renames the column according to the specified alias if provided.
     */
    fun typed(): Dataset<Row> {
        // build a list of Columns to select with the casting or transforms to achieve the required type
        val typedColumns: List<Column> =
            fileSource.table.columns.map { column ->
                // convert to type
                types.get(column.type).process(column.alias, column.formats)
            }
        // call var args function https://stackoverflow.com/a/65520425
        return selected().select(*typedColumns.map { it }.toTypedArray())
    }

    /**
     * Builds a valid dataset by filtering out rows that are missing required values, and de-duplicating if required.
     */
    fun valid(deduplicate: Boolean = true): Dataset<Row> {

        // required columns != null conditions
        val requiredColumns: List<Column> = fileSource.table.columns
            .filter { column -> column.required }
            .map { column ->
                // for strings, check for null and empty strings
                if (string.key() == column.type)
                    col(column.alias).isNotNull.and(
                        trim(col(column.alias)).notEqual(functions.lit(""))
                    )
                else
                // otherwise just check for null
                    col(column.alias).isNotNull
            }

        // and all columns together so none of the required columns can be null
        var combined: Column? = null
        requiredColumns.forEach { col ->
            combined = if (combined == null) col else combined!!.and(col)
        }

        // select all columns where the required columns are not null
        val dataset = typed().select("*")
            .where(combined)

        // handle deduplication
        return if (deduplicate) {
            dataset.dropDuplicates()
        } else {
            dataset
        }
    }

}