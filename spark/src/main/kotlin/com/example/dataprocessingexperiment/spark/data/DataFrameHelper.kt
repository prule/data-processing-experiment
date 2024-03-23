package com.example.dataprocessingexperiment.spark.data

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class DataFrameHelper(private val data: Dataset<Row>) {
    fun stringColumns() {
//        data.dtypes().filter { it. }
    }
}