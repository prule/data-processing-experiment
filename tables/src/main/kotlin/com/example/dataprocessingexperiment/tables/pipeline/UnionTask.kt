package com.example.dataprocessingexperiment.tables.pipeline

import kotlinx.serialization.Serializable

@Serializable
data class UnionTask(
    override val id: String,
    override val name: String,
    override val description: String,

    val destination: String,
    val tables: List<String>
): AbstractTask() {
}