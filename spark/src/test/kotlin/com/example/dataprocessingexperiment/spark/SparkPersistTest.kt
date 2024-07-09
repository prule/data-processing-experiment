package com.example.dataprocessingexperiment.spark

import io.kotest.matchers.longs.shouldBeGreaterThan
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.junit.jupiter.api.Test

class SparkPersistTest {

    @Test
    fun `spark should join csv`() {

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load raw data frame
        val table1 = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load("../data/simple-join/left.csv")

        println(1)
        val table2 = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load("../data/simple-join/right.csv")

        println(2)
        val table = table1.join(table2, "id")

        println(3)
        table.write()
            .option("header", "true")
            .format("csv")
            .mode("overwrite")
            .save("./build/output/temp")
    }

    @Test
    fun `spark should read csv`() {

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load raw data frame
        val table = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load("../data/reference-app-1/transactions/2020-03.csv")
            .alias("transactions")
//            .cache()

//        table.show() // introduces an extra read if we don't use persist or cache

        table.write()
            .option("header", "true")
            .format("csv")
            .mode("overwrite")
            .save("./build/output/temp")
    }


    @Test
    fun `spark should read csv 5 times`() {

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // load raw data frame
        val statementsDataFrame = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load("../data/reference-app-1/transactions/2020-03.csv")
            .alias("transactions")

        // without persist the CSV gets read 5 times - this line appears 5 time in the logs
        // 24/07/08 12:17:00 INFO FileScanRDD: Reading File path: file:///Users/paulrule/IdeaProjects/data-processing-experiment-2/data/reference-app-1/transactions/2020-03.csv, range: 0-315, partition values: [empty row]

        // with persist, its only seen twice
        statementsDataFrame.persist()

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

}