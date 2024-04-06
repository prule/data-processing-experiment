package com.example.dataprocessingexperiment.spark

import io.kotest.matchers.longs.shouldBeGreaterThan
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.*
import org.junit.jupiter.api.Disabled
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
            col("date"),
            col("account"),
            col("description"),
            col("amount"),
        )

        println("Selected data frame")
        selectedDataFrame.show(20)
        selectedDataFrame.printSchema()

        // convert to typed columns
        val typedDataFrame = selectedDataFrame.select(
            to_date(trim(col("date")), "yyyy-MM-dd").alias("amount"),
            col("account"),
            col("description"),
            col("amount").cast("double").alias("amount")
        )

        println("Typed data frame")
        typedDataFrame.show(20)
        typedDataFrame.printSchema()

        typedDataFrame.count() shouldBeGreaterThan 0

    }


    /**
     * Prepare data for the sample - with different column names and limited rows.
     * Need to execute `./run data:download` to source the raw data first.
     * This is a once off operation.
     */
    @Test
    @Disabled
    fun `transform lga data`() {

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load raw data frame
        val dataFrame = sparkSession.read()
            .format("csv")
            .option("header", true)
            .option("delimiter", ";")
            .load("../data/sample1/downloaded/aus-lga-mapping/")
            .alias("lgas")

        println("data frame")
        dataFrame.show(20)
        dataFrame.printSchema()

        val path = "../data/sample1/downloaded/transformed/"

        // only select the columns needed so we can exclude data we don't need here
        val selectedDataFrame = dataFrame.select(
            "Year",
            "Official Code State",
            "Official Name State",
            "Official Code Local Government Area",
            "Official Name Local Government Area",
            "Iso 3166-3 Area Code",
            "Type",
            "Long Official Name Local Government Area"
        )

        // write CSVs for 3 states with different columns in different orders

        // 1
        selectedDataFrame.select(
            "Official Code State",
            "Official Name State",
            "Year",
            "Official Code Local Government Area",
            "Iso 3166-3 Area Code",
            "Official Name Local Government Area",
            "Type",
            "Long Official Name Local Government Area"
        )
            .filter(col("Official Code State").equalTo(1))
            .limit(10)
            .write()
            .option("header", true)
            .option("delimiter", ";")
            .mode(SaveMode.Overwrite)
            .format("csv")
            .save(path + 1)

        // 2
        selectedDataFrame.select(
            "Official Name State",
            "Year",
            "Official Code Local Government Area",
            "Official Code State",
            "Iso 3166-3 Area Code",
            "Official Name Local Government Area",
        )
            .filter(col("Official Code State").equalTo(2))
            .limit(10)
            .write()
            .option("header", true)
            .option("delimiter", ";")
            .mode(SaveMode.Overwrite)
            .format("csv")
            .save(path + 2)


        // 3
        selectedDataFrame.select(
            "Official Name State",
            "Official Code Local Government Area",
            "Official Code State",
            "Iso 3166-3 Area Code",
            "Official Name Local Government Area",
        )
            .filter(col("Official Code State").equalTo(3))
            .limit(10)
            .write()
            .option("header", true)
            .option("delimiter", ";")
            .mode(SaveMode.Overwrite)
            .format("csv")
            .save(path + 3)

    }
}