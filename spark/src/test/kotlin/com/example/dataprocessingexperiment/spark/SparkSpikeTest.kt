package com.example.dataprocessingexperiment.spark

import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.junit.jupiter.api.Test

/**
 * A basic class that sets the pattern for Spark SQL processing.
 *  - reads a dataframe from a folder containing CSVs
 *  - selects the required columns
 *  - converts the columns to the desired types
 */
class SparkSpikeTest {

    @Test
    fun `spark sql should work`() {

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load raw data frame
        val statementsDataFrame = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load("../data/sample1/transactions")
            .alias("transactions")

        println("Statements data frame")
        statementsDataFrame.show(20)
        statementsDataFrame.printSchema()

        // only select the columns needed so we can exclude data we don't need here
        val selectedDataFrame = statementsDataFrame.select(
            functions.col("date"),
            functions.col("account"),
            functions.col("description"),
            functions.col("amount"),
        )

        println("Selected data frame")
        selectedDataFrame.show(20)
        selectedDataFrame.printSchema()

        // convert to typed columns
        val typedDataFrame = selectedDataFrame.select(
            functions.to_date(functions.col("date"), "yyyy-MM-dd").alias("amount"),
            functions.col("account"),
            functions.col("description"),
            functions.col("amount").cast("double").alias("amount")
        )

        println("Typed data frame")
        typedDataFrame.show(20)
        typedDataFrame.printSchema()

        typedDataFrame.count() shouldBeGreaterThan 0

    }

}