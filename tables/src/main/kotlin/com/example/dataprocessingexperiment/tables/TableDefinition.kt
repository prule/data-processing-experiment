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
    val delimiter: String = ",",
    val columns: List<ColumnDefinition>
) {
    fun colByName(name: String): ColumnDefinition {
        return columns.first { col -> col.names.contains(name) }
    }
}
