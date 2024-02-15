package com.example.dataprocessingexperiment.spark.types

import org.apache.spark.sql.Column

interface Typer {
    fun key(): String
    fun process(name: String, type: String, formats: List<String>?): Column
}