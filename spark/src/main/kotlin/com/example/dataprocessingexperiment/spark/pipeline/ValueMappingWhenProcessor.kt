package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import kotlinx.serialization.Serializable
import org.apache.orc.impl.ParserUtils.findColumn
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.`when`

/**
 * Maps column values as per a mapping configuration using WHEN clauses.
 *
 * Given a mapping configuration:
 * ```
 * [
 *   {
 *     tableId: "transactions",
 *     columns: [
 *       {
 *         columnName: "description",
 *         mapping: {
 *           value: "burger",
 *           alternatives: [
 *             "burgers"
 *           ]
 *         }
 *       }
 *     ]
 *   }
 * ]
 * ```
 * And a "transactions" table:
 * ```
 * +----+-----------+----+
 * |val1|description|val2|
 * +----+-----------+----+
 * |   a|    burgers|   1|
 * |   b|     burger|   2|
 * |   c|      apple|   3|
 * |   d|       NULL|   4|
 * +----+-----------+----+
 * ```
 * Mapping produces standard values ("burger" instead of "burgers" in description column):
 * ```
 * +----+-----------+----+
 * |val1|description|val2|
 * +----+-----------+----+
 * |   a|     burger|   1|
 * |   b|     burger|   2|
 * |   c|      apple|   3|
 * |   d|       NULL|   4|
 * +----+-----------+----+
 * ```
 */
@Serializable
class ValueMappingWhenProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    private val mappings: List<TableMapping>
) : Processor {
    override fun process(context: SparkContext) {

        for (mapping in this.mappings) {

            val tableToMap = context.get(mapping.tableId).alias(mapping.tableId)
            val criteria = mapping.criteria(tableToMap.columns().toList())
            val result = tableToMap.select(*criteria.toTypedArray()).let {
                if (mapping.deduplicate()) it.dropDuplicates() else it
            }

            // replace in context
            context.set(mapping.tableId, result)

        }
    }

    override fun toString(): String {
        return "ValueMappingWhenProcessor(id='$id', name='$name', description='$description', mappings='$mappings')"
    }

    @Serializable
    data class TableMapping(val tableId: String, val deduplicate: Boolean? = false, val columns: List<ColumnMapping>) {
        fun deduplicate(): Boolean {
            return deduplicate ?: false
        }

        fun criteria(allColumns: List<String>): List<Column> {
            val columnsToSelect: MutableList<Column> = mutableListOf()
            for (column in allColumns) {
                columnsToSelect.add(findColumn(column)?.criteria() ?: col(column))
            }
            return columnsToSelect.toList()
        }

        private fun findColumn(column: String): ColumnMapping? {
            return columns.firstOrNull { it.columnName == column }
        }

    }

    @Serializable
    data class ColumnMapping(val columnName: String, val mapping: ValueMapping) {
        fun criteria(): Column {
            return mapping.criteria(col(columnName)).alias(columnName)
        }
    }

    @Serializable
    data class ValueMapping(val value: String, val alternatives: List<String>) {
        fun criteria(col: Column): Column {
            var criteria: Column? = null
            if (alternatives.isEmpty()) {
                return col
            } else {
                alternatives.map {
                    criteria = if (criteria == null) {
                        `when`(col.equalTo(it), value)
                    } else {
                        criteria!!.`when`(col.equalTo(it), value)
                    }
                }
                return criteria!!.otherwise(col)
            }
        }
    }
}