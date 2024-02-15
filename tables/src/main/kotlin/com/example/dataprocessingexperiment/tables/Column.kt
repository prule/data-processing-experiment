package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

@Serializable
data class Column(
    val name: String,
    val description: String,
    val type: String,
    val formats: List<String>? = listOf()
)
