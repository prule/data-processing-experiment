package com.example.dataprocessingexperiment.spark

import org.apache.spark.sql.Dataset

class DatasetOutput(private val displayRows: Int = 10, private val truncate: Int = 20) {
    fun show(df: Dataset<*>) {
        if (displayRows > 0) {
            df.show(displayRows, truncate)
        }
    }
}