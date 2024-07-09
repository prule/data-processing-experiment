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

        table.show()

        // it knows not to duplicate the "id" column

//        +---+---------+-------+--------+---------------+
//        | id|firstname|   desc|lastname|           desc|
//        +---+---------+-------+--------+---------------+
//        |  1|    Homer|    Dad| Simpson| Simpson family|
//        |  2|    Marge| Mother| Simpson| Simpson family|
//        +---+---------+-------+--------+---------------+
    }

    "spark should join csv with renamed column" {

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
        val table = table1.join(table2.withColumnRenamed("id","id2"), col("id").equalTo(col("id2")))

        println(3)
        table.write()
            .option("header", "true")
            .format("csv")
            .mode("overwrite")
            .save("./build/output/temp")

        table.show()

        // we get both "id" and "id2" columns here

//        +---+---------+-------+---+--------+---------------+
//        | id|firstname|   desc|id2|lastname|           desc|
//        +---+---------+-------+---+--------+---------------+
//        |  1|    Homer|    Dad|  1| Simpson| Simpson family|
//        |  2|    Marge| Mother|  2| Simpson| Simpson family|
//        +---+---------+-------+---+--------+---------------+
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

    "spark" {

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
