package com.example.dataprocessingexperiment.tables.pipeline

import kotlinx.serialization.Serializable

@Serializable
data class JoinTaskDefinition(
//    override val type: String,
    override val id: String,
    override val name: String,
    override val description: String,
//
    val table1: String,
    val table2: String,
    val destination: String,
    val joinType: String,
    val on: Map<String, String>,
    val columns: List<String>
): AbstractTaskDefinition() {
}