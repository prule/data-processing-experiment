package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

/**
 * Column defines the name (which must match the raw data), the type of the column (eg integer, decimal, boolean etc),
 * and optionally formats which can be used by the type to know how to parse.
 *
 * ```
 *       {
 *         name: "date",
 *         description: "date of transaction",
 *         type: "date",
 *         formats: [
 *           "yyyy-MM-dd",
 *           "dd-MM-yyyy"
 *         ],
 *         required: true
 *       }
 * ```
 */
@Serializable
data class ColumnDefinition(
    val names: List<String>,
    val alias: String,
    val description: String,
    val type: String,
    val formats: List<String>? = listOf(),
    val required: Boolean = false
)
