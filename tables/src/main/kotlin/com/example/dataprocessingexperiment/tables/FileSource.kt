package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

/**
 * FileSource defines the location of the table data and the table structure.
 *
 * ```
 * {
 *   name: "sample 1",
 *   description: "sample 1 description",
 *   path: "sample1/statements/",
 *   type: "csv",
 *   table: {
 *      ...
 *   }
 * }
 * ```
 */
@Serializable
class FileSource(
    val name: String,
    val description: String,
    val path: String,
    val type: String,
    val table: Table,
)