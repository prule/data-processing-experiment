package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.types.Types
import com.example.dataprocessingexperiment.tables.FileSource
import io.github.xn32.json5k.Json5
import io.github.xn32.json5k.decodeFromStream
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


class App {

    fun go() {
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        sparkSession.use {

            val fileSource = Json5.decodeFromStream<FileSource>(
                this::class.java.getResourceAsStream("/sample1.statements.json5")!!
            )

            // set up the dataframe
            val dataFrameBuilder = DataFrameBuilder(
                sparkSession,
                fileSource,
                Types.all(),
                "../data/"
            )

            // get the raw version of the dataset, everything is a string, and all columns are included
            display("Raw dataset",dataFrameBuilder.raw)

            /*

            In the raw dataset, every column is represented as a string.

                root
                 |-- date: string (nullable = true)
                 |-- account: string (nullable = true)
                 |-- description: string (nullable = true)
                 |-- amount: string (nullable = true)
                 |-- comment: string (nullable = true)

                +------------+-------+------------+-------+--------------------+
                |        date|account| description| amount|             comment|
                +------------+-------+------------+-------+--------------------+
                |        NULL|      x|      tennis|   0.03|             no date|
                |  01-03-2020|      1|      burger|  15.47|alternative date ...|
                |  03-03-2020|      1|      tennis|  35.03|alternative date ...|
                |  04-03-2020|      2|      petrol| 150.47|alternative date ...|
                |  2020-01-01|      1|      burger|  15.45|                NULL|
                |  2020-01-02|      1|       movie|  20.00|                NULL|
                |  2020-01-03|      1|      tennis|  35.00|                NULL|
                |  2020-01-04|      2|      petrol| 150.45|                NULL|
                |  2020-02-01|      1|      burger|  15.46|                NULL|
                |  2020-02-02|      1|       movie|  20.01|                NULL|
                |  2020-02-03|      1|      tennis|  35.01|                NULL|
                |  2020-02-04|      2|      petrol| 150.46|                NULL|
                |  2020-02-04|      2| electricity| 300.47|                NULL|
                |  2020-12-01|       |      tennis|   0.04| blank (many spac...|
                |  2020-12-01|      x|      petrol|      x| invalid number f...|
                |  2020-13-01|      x|      burger|   0.01|        invalid date|
                |invalid date|      x|      petrol|   0.02|        invalid date|
                |           x|      x|           x|      x| row with multipl...|
                +------------+-------+------------+-------+--------------------+

                row count = 18

             */

            // get the typed version of the dataset, with columns and types specified in config
            display("Typed dataset",dataFrameBuilder.typed())

            /*

            When values can't be converted to their proper type, they'll appear as NULL.

                root
                 |-- date: date (nullable = true)
                 |-- account: string (nullable = true)
                 |-- description: string (nullable = true)
                 |-- amount: decimal(10,2) (nullable = true)

                +----------+-------+------------+------+
                |      date|account| description|amount|
                +----------+-------+------------+------+
                |      NULL|      x|      burger|  0.01|
                |      NULL|      x|      petrol|  0.02|
                |      NULL|      x|      tennis|  0.03|
                |      NULL|      x|           x|  NULL|
                |2020-01-01|      1|      burger| 15.45|
                |2020-01-02|      1|       movie| 20.00|
                |2020-01-03|      1|      tennis| 35.00|
                |2020-01-04|      2|      petrol|150.45|
                |2020-02-01|      1|      burger| 15.46|
                |2020-02-02|      1|       movie| 20.01|
                |2020-02-03|      1|      tennis| 35.01|
                |2020-02-04|      2|      petrol|150.46|
                |2020-02-04|      2| electricity|300.47|
                |2020-03-01|      1|      burger| 15.47|
                |2020-03-03|      1|      tennis| 35.03|
                |2020-03-04|      2|      petrol|150.47|
                |2020-12-01|       |      tennis|  0.04|
                |2020-12-01|      x|      petrol|  NULL|
                +----------+-------+------------+------+

                row count = 18

            */

            display("Valid dataset",dataFrameBuilder.valid())

            /*

            We can remove any rows where a required column is null - consider these invalid.

                root
                 |-- date: date (nullable = true)
                 |-- account: string (nullable = true)
                 |-- description: string (nullable = true)
                 |-- amount: decimal(10,2) (nullable = true)

                +----------+-------+------------+------+
                |      date|account| description|amount|
                +----------+-------+------------+------+
                |2020-01-01|      1|      burger| 15.45|
                |2020-01-02|      1|       movie| 20.00|
                |2020-01-03|      1|      tennis| 35.00|
                |2020-01-04|      2|      petrol|150.45|
                |2020-02-01|      1|      burger| 15.46|
                |2020-02-02|      1|       movie| 20.01|
                |2020-02-03|      1|      tennis| 35.01|
                |2020-02-04|      2|      petrol|150.46|
                |2020-02-04|      2| electricity|300.47|
                |2020-03-01|      1|      burger| 15.47|
                |2020-03-03|      1|      tennis| 35.03|
                |2020-03-04|      2|      petrol|150.47|
                +----------+-------+------------+------+

                row count = 12

             */
        }



    }

    private fun display(name: String, ds: Dataset<Row>) {
        println()
        println(name)
        println()
        ds.printSchema()
        ds.orderBy("date").show(20)
        println("row count = ${ds.count()}")
    }
}

fun main() {
    println("Starting...")

    App().go()

    println("Finished...")
}
