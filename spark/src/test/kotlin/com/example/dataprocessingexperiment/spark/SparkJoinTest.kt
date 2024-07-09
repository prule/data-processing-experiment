package com.example.dataprocessingexperiment.spark

import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.longs.shouldBeGreaterThan
import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SparkJoinTest : StringSpec({


    "spark should join csv" {

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

    "spark should fail because column exists" {

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

        // expect exception
        shouldThrowMessage("[COLUMN_ALREADY_EXISTS] The column `id` already exists. Consider to choose another name or rename the existing column.") {
            // if we don't tell it to use col="id" we get
            // org.apache.spark.sql.AnalysisException: [COLUMN_ALREADY_EXISTS] The column `id` already exists. Consider to choose another name or rename the existing column.
            val table = table1.join(table2)
            // need to write to invoke the sql join and experience the error
            table.write()
                .option("header", "true")
                .format("csv")
                .mode("overwrite")
                .save("./build/output/temp")
        }

    }

})
