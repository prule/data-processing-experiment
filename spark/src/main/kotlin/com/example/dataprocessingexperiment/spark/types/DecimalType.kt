package com.example.dataprocessingexperiment.spark.types

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

class DecimalType : Typer {
    override fun key(): String {
        return "decimal"
    }

    override fun process(name: String, type: String, formats: List<String>?): Column {
        var typeCast = "decimal"
        if (formats != null) {
            if (formats.isNotEmpty()) {
                typeCast = "decimal(${formats[0]})"
            }
        }
        return functions.col(name).cast(typeCast).alias(name)
    }
}