package com.example.dataprocessingexperiment.tables.pipeline

import kotlinx.serialization.Serializable

@Serializable
data class LiteralTaskDefinition(
//    override val type: String,
    override val id: String,
    override val name: String,
    override val description: String,
//
    val table: String,
    val columns: Map<String, String>, // TODO need to be able to add literals that aren't strings?
): AbstractTaskDefinition()