package com.example.dataprocessingexperiment.spark.data.types

import kotlinx.serialization.Serializable
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

@Serializable
class NoOpType: Typer {
    override fun key(): String {
        return "noop"
    }

    override fun process(name: String): Column {
        return col(name)
    }
}