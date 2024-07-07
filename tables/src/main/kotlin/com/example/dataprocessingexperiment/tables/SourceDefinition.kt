package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

/**
 * SourceDefinition defines the location of the table data and the table structure.
 *
 * ```
 * {
 *   name: "sample 1",
 *   description: "sample 1 description",
 *   path: "reference-app-1/statements/",
 *   type: "csv",
 *   table: {
 *      ...
 *   }
 * }
 * ```
 */
@Serializable
class SourceDefinition(
    val id: String,
    val name: String,
    val description: String,
    val path: String,
    val type: SourceType,
    val table: TableDefinition,
)