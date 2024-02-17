package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.types.Types
import com.example.dataprocessingexperiment.tables.FileSource
import io.github.xn32.json5k.Json5
import io.github.xn32.json5k.decodeFromStream
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class App {

    fun go() {
        val fileSource = Json5.decodeFromStream<FileSource>(
            this::class.java.getResourceAsStream("/sample1.statements.json5")!!
        )

        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // set up the dataframe
        val dataFrameBuilder = DataFrameBuilder(
            sparkSession,
            fileSource,
            Types.all(),
            "../data/"
        )

        // get the raw version of the dataset, everything is a string, and all columns are included
        val rawDataset = dataFrameBuilder.raw
        rawDataset.printSchema()
        rawDataset.show(20)

        /*

            root
             |-- date: string (nullable = true)
             |-- account: string (nullable = true)
             |-- description: string (nullable = true)
             |-- amount: string (nullable = true)
             |-- comment: string (nullable = true)

            +------------+-------+------------+-------+--------------------+
            |        date|account| description| amount|             comment|
            +------------+-------+------------+-------+--------------------+
            |  2020-13-01|      x|      burger|   0.01|        invalid date|
            |invalid date|      x|      petrol|   0.02|        invalid date|
            |        NULL|      x|      tennis|   0.03|             no date|
            |  2020-12-01|       |      tennis|   0.04|          no account|
            |  2020-12-01|      x|      petrol|      x| invalid number f...|
            |  01-03-2020|      1|      burger|  15.47|alternative date ...|
            |  03-03-2020|      1|      tennis|  35.03|alternative date ...|
            |  04-03-2020|      2|      petrol| 150.47|alternative date ...|
            |  2020-02-01|      1|      burger|  15.46|                NULL|
            |  2020-02-02|      1|       movie|  20.01|                NULL|
            |  2020-02-03|      1|      tennis|  35.01|                NULL|
            |  2020-02-04|      2|      petrol| 150.46|                NULL|
            |  2020-02-04|      2| electricity| 300.47|                NULL|
            |  2020-01-01|      1|      burger|  15.45|                NULL|
            |  2020-01-02|      1|       movie|  20.00|                NULL|
            |  2020-01-03|      1|      tennis|  35.00|                NULL|
            |  2020-01-04|      2|      petrol| 150.45|                NULL|
            +------------+-------+------------+-------+--------------------+

         */

        // get the typed version of the dataset, with columns and types specified in config
        val typedDataset = dataFrameBuilder.typed()
        typedDataset.printSchema()
        typedDataset.show(20)

        /*

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
            |2020-12-01|       |      tennis|  0.04|
            |2020-12-01|      x|      petrol|  NULL|
            |2020-03-01|      1|      burger| 15.47|
            |2020-03-03|      1|      tennis| 35.03|
            |2020-03-04|      2|      petrol|150.47|
            |2020-02-01|      1|      burger| 15.46|
            |2020-02-02|      1|       movie| 20.01|
            |2020-02-03|      1|      tennis| 35.01|
            |2020-02-04|      2|      petrol|150.46|
            |2020-02-04|      2| electricity|300.47|
            |2020-01-01|      1|      burger| 15.45|
            |2020-01-02|      1|       movie| 20.00|
            |2020-01-03|      1|      tennis| 35.00|
            |2020-01-04|      2|      petrol|150.45|
            +----------+-------+------------+------+

        */

    }
}

fun main() {
    println("Starting...")

    App().go()

    println("Finished...")
}
