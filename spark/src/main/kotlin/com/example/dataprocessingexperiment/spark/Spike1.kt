package com.example.dataprocessingexperiment.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.functions

/**
 * A basic class that sets the pattern for Spark SQL processing.
 *  - reads a dataframe from a folder containing CSVs
 *  - selects the required columns
 *  - converts the columns to the desired types
 *
 * I don't like this however, because everything is hardcoded and nothing can be reused/leveraged.
 *  - file location and type
 *  - column names
 *  - column types
 *  - date format
 *
 *  Let's start addressing that in part-3.
 */
class Spike1 {

    fun run() {

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load raw data frame
        val statementsDataFrame = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load("../data/sample1/statements")
            .alias("statements")

        println("Statements data frame")
        statementsDataFrame.show()
        statementsDataFrame.printSchema()

        // only select the columns needed so we can exclude data we don't need here
        val selectedDataFrame = statementsDataFrame.select(
            functions.col("date"),
            functions.col("account"),
            functions.col("description"),
            functions.col("amount"),
        )

        println("Selected data frame")
        selectedDataFrame.show()
        selectedDataFrame.printSchema()

        // convert to typed columns
        val typedDataFrame = selectedDataFrame.select(
            functions.to_date(functions.col("date"), "yyyy-MM-dd").alias("amount"),
            functions.col("account"),
            functions.col("description"),
            functions.col("amount").cast("double").alias("amount")
        )

        println("Typed data frame")
        typedDataFrame.show()
        typedDataFrame.printSchema()

    }

}