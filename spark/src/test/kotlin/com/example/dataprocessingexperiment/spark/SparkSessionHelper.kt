package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.spark.types.DecimalTypeTest
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionHelper {
    var sparkSession: SparkSession? = null

    fun open(): SparkSession {
        val config = SparkConf().setAppName("spike").setMaster("local")
        sparkSession = SparkSession.builder().config(config).orCreate
        return sparkSession!!
    }

    fun close() {
        sparkSession!!.close()
    }
}