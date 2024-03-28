package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col

/**
 * Maps column values as per a mapping table by joining the 2 tables.
 *
 * Given a mapping table:
 *
 * +------------+-----------+-------+------+
 * |       table|     column|   from|    to|
 * +------------+-----------+-------+------+
 * |transactions|description|burgers|burger|
 * +------------+-----------+-------+------+
 *
 * And a "transactions" table:
 *
 * +----+-----------+----+
 * |val1|description|val2|
 * +----+-----------+----+
 * |   a|    burgers|   1|
 * |   b|     burger|   2|
 * |   c|      apple|   3|
 * |   d|       NULL|   4|
 * +----+-----------+----+
 *
 * Mapping produces:
 *
 * +----+-----------+----+
 * |val1|description|val2|
 * +----+-----------+----+
 * |   a|     burger|   1|
 * |   b|     burger|   2|
 * |   c|      apple|   3|
 * |   d|       NULL|   4|
 * +----+-----------+----+
 */
@Serializable
class ValueMappingJoinProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val tables: List<String>
) : Processor {
    override fun process(context: SparkContext) {

        for (table in this.tables) {

            val mapping = context.get(table)
            val tablesWithMappings = mapping.select("table").distinct().collectAsList().map { it.getString(0) }

            for (tableId in tablesWithMappings) {

                val tableToMap = context.get(tableId).alias(tableId)
                val mappingsForThisTable = mapping.select(col("column"), col("from"), col("to")).where(col("table").equalTo(tableId))
                val columnsWithMappings = mappingsForThisTable.select("column").distinct().collectAsList().map { it.getString(0) }

                for (column in columnsWithMappings) {
                    val mappingsForThisColumn = mappingsForThisTable.select(col("from"), col("to")).where(col("column").equalTo(column)).alias("mapping")
                    val updatedTable = updateTable(tableToMap, mappingsForThisColumn, tableId, column)
                    // replace in context
                    context.set(tableId, updatedTable)
                }
            }


        }
    }

    /**
     * Given a table:
     *
     * +----+-----------+----+
     * |val1|description|val2|
     * +----+-----------+----+
     * |   a|    burgers|   1|
     * |   b|     burger|   2|
     * |   c|      apple|   3|
     * |   d|       NULL|   4|
     * +----+-----------+----+
     *
     * With mappings for the "description" column:
     *
     * +-------+------+
     * |   from|    to|
     * +-------+------+
     * |burgers|burger|
     * +-------+------+
     *
     * The joined table looks like:
     *
     * +----+-----------+----+-------+------+
     * |val1|description|val2|   from|    to|
     * +----+-----------+----+-------+------+
     * |   a|    burgers|   1|burgers|burger|
     * |   b|     burger|   2|   NULL|  NULL|
     * |   c|      apple|   3|   NULL|  NULL|
     * |   d|       NULL|   4|   NULL|  NULL|
     * +----+-----------+----+-------+------+
     *
     * Now select the first non-null value between "to" and "description" and select the remaining original columns to get the result
     *
     * +----+-----------+----+
     * |val1|description|val2|
     * +----+-----------+----+
     * |   a|     burger|   1|
     * |   b|     burger|   2|
     * |   c|      apple|   3|
     * |   d|       NULL|   4|
     * +----+-----------+----+
     */

    private fun updateTable(
        tableToMap: Dataset<Row>,
        mappingsForThisColumn: Dataset<Row>,
        tableId: String,
        column: String
    ): Dataset<Row> {

        // join mappings to main table
        val joined =
            tableToMap.join(
                mappingsForThisColumn,
                col("$tableId.$column").equalTo(col("mapping.from")),
                "left"
            )

        // take either the mapped column if it isn't null or the original column
        val newColumn = functions.coalesce(col("to"), col("$tableId.$column")).alias(column)
        val allColumns: List<Column> = tableToMap.columns().toList().map { it ->
            if (it.equals(column)) {
                newColumn
            } else {
                col(it)
            }
        }

        // get the columns as per the original table
        return joined.select(*allColumns.map { it }.toTypedArray())
    }

    override fun toString(): String {
        return "ValueMappingProcessor(id='$id', name='$name', description='$description', tables=$tables)"
    }

}