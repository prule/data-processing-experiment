package com.example.dataprocessingexperiment.tables

import kotlinx.serialization.Serializable

@Serializable
data class Table(
    val name:String,
    val description:String,
    val columns: List<Column>
) {
    fun colByName(name: String): Column {
        return columns.first { col -> col.name == name }
    }
}
