package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

@Serializable
class FileSource(
    val name: String,
    val description: String,
    val path: String,
    val type: String,
    val table: Table,
)