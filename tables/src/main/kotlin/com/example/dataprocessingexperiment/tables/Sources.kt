package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

/**
 * ```
 * {
 *   id: "sample1",
 *   name: "Sample 1",
 *   description: "Sample 1 is a basic dataset configuration to demonstrate capability",
 *   sources: [
 *     ...
 *   ]
 * }
 * ```
 */
@Serializable
data class Sources(
    val id: String,
    val name: String,
    val description: String,
    val sources: List<SourceDefinition>
) {
    fun sourceById(id: String): SourceDefinition {
        return sources.first { source -> source.id == id }
    }
}
