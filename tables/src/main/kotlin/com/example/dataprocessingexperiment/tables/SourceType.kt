package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

@Serializable
data class SourceType(
    val format: String,
    val options: Map<String, String> = mapOf()
) {

}