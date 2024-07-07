package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

/**
 * Table defines the name and columns.
 *
 * ```
 *   table: {
 *     name: "transactions",
 *     description: "account transactions",
 *     columns: [
 *       ...
 *     ]
 *   }
 * ```
 */
@Serializable
data class TableDefinition(
    val name: String,
    val description: String,
    val deduplicate: Boolean = false,
    val columns: List<ColumnDefinition>,
    val trim: Boolean? = false,
) {
    fun colByName(name: String): ColumnDefinition {
        return columns.first { col -> col.names.contains(name) }
    }

    fun trim(columnName: String): Boolean {
        return if (colByName(columnName).trim != null) {
            colByName(columnName).trim!!
        } else {
            trim ?: false
        }
    }
}
