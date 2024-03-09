package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

/**
 * Table defines the name and columns.
 *
 * ```
 * {
 *   id: "sample1",
 *   name: "Sample 1",
 *   description: "Sample 1 is a basic dataset configuration to demonstrate capability",
 *   tables: [
 *     ...
 *   ]
 * }
 * ```
 */
@Serializable
data class Tables(
    val id: String,
    val name: String,
    val description: String,
    val sources: List<FileSource>
) {
    fun tableById(id: String): FileSource {
        return sources.first { source -> source.id == id }
    }
}
