package com.example.dataprocessingexperiment.tables.pipeline

import kotlinx.serialization.Serializable

@Serializable
abstract class AbstractTask {
//    abstract val type: String
    abstract val id: String
    abstract val name: String
    abstract val description: String
}