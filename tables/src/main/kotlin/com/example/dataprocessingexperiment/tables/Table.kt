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
data class Table(
    val name: String,
    val description: String,
    val deduplicate: Boolean = false,
    val columns: List<Column>
) {
    fun colByName(name: String): Column {
        return columns.first { col -> col.name == name }
    }
}
