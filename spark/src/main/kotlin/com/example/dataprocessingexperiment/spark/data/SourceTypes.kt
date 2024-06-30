package com.example.dataprocessingexperiment.spark.data

import com.example.dataprocessingexperiment.spark.data.SourceTypes.types
import com.example.dataprocessingexperiment.tables.SourceDefinition
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object SourceTypes {
    val types = mapOf<String, (SourceDefinition, Dataset<Row>) -> Dataset<Row>> (
//        "csv" to { sourceDefinition, df -> handleCsv(sourceDefinition, df) },
        "default" to { sourceDefinition, df -> handleDefault(sourceDefinition, df) }
    )


    fun get(format: String): (SourceDefinition, Dataset<Row>) -> Dataset<Row> {
        return if (types.containsKey(format)) {
            types[format]!!
        } else {
            types["default"]!!
        }
    }

    private fun handleCsv(sourceDefinition: SourceDefinition, df: Dataset<Row>): Dataset<Row> {
        return if (!sourceDefinition.type.options.containsKey("header") || sourceDefinition.type.options["header"] == "false") {
            df.show()
            df.toDF(*(sourceDefinition.table.columns.map { it.alias }.toTypedArray()))
        } else {
            df
        }
    }

    private fun handleDefault(sourceDefinition: SourceDefinition, df: Dataset<Row>): Dataset<Row> {
        return df
    }
}