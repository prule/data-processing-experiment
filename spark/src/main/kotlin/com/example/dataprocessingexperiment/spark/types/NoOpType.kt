package com.example.dataprocessingexperiment.spark.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class NoOpType: Typer {
    override fun key(): String {
        return "noop"
    }

    override fun process(name: String, type: String, formats: List<String>?): Column {
        return col(name)
    }
}