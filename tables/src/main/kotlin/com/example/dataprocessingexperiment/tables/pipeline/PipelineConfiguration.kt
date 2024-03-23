package com.example.dataprocessingexperiment.tables.pipeline

import kotlinx.serialization.Serializable

@Serializable
class PipelineConfiguration(
    val id: String,
    val name: String,
    val description: String,
    val tasks: List<ProcessorDefinition>
    ) {

}