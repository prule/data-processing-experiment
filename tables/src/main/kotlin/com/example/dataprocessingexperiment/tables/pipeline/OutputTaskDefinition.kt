package com.example.dataprocessingexperiment.tables.pipeline

import kotlinx.serialization.Serializable

@Serializable
data class OutputTaskDefinition(
    override val id: String,
    override val name: String,
    override val description: String,
    val table: String,
    val path: String,
    val format: String,
    val mode: String,
    val options: Map<String, String>
): AbstractTaskDefinition()