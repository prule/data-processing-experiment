package com.example.dataprocessingexperiment.spark.types

import com.example.dataprocessingexperiment.spark.functions.Date
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

class DateType : Typer {
    override fun key(): String {
        return "date"
    }

    override fun process(name: String, type: String, formats: List<String>?): Column {
        return Date(formats!!).parse(functions.col(name)).alias(name)
    }
}