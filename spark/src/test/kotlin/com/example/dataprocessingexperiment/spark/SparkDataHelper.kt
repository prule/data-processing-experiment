package com.example.dataprocessingexperiment.spark

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class SparkDataHelper(private val sparkSession: SparkSession, private val isNullable: Boolean = false) {

    fun asDataFrame(data: List<GenericRow>, cols: List<Pair<String, DataType>>): Dataset<Row> {
        return sparkSession.createDataFrame(
            data, StructType(
                arrayOf(
                    *cols.map {
                        StructField(
                            it.first, it.second, isNullable, Metadata.empty()
                        )
                    }.toTypedArray()
                )
            )
        )
    }
}