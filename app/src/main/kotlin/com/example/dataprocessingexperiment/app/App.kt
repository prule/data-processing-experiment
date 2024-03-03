package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.data.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.data.types.Types
import com.example.dataprocessingexperiment.spark.statistics.StatisticsRunner
import com.example.dataprocessingexperiment.spark.statistics.StatisticRepository
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import com.example.dataprocessingexperiment.tables.Tables
import com.example.dataprocessingexperiment.tables.statistics.Statistics
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import io.github.xn32.json5k.Json5
import io.github.xn32.json5k.decodeFromStream
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 * Reference implementation to demonstrate and exercise current capabilities.
 */

class App {
    private val displayRows = 20

    fun go() {
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // clean up the output directory
        val outputPath = "./build/out/sample1/statements/statistics"
        File(outputPath).deleteRecursively()

        // load configuration
        val tables = Json5.decodeFromStream<Tables>(
            this::class.java.getResourceAsStream("/sample1.tables.json5")!!
        )

        val statisticConfiguration = Json5.decodeFromStream<StatisticsConfiguration>(
            this::class.java.getResourceAsStream("/sample1.statistics.json5")!!
        )

        // run
        // load each table
        //  generate statistics if configured
        sparkSession.use {

            tables.sources.forEach { fileSource ->

                // set up the dataframe
                val dataFrameBuilder = DataFrameBuilder(
                    sparkSession,
                    fileSource,
                    Types.all(),
                    "../data/"
                )

                // get the raw version of the dataset, everything is a string, and all columns are included
                val rawDataset = dataFrameBuilder.raw
                display("Raw dataset", rawDataset, "date")

                // statistics
                val stats = statisticConfiguration.statisticsById(fileSource.id)
                stats?.let {
                    val rawStatisticsPath = "$outputPath/${fileSource.id}/raw"
                    generateStatistics(stats, rawDataset, rawStatisticsPath, sparkSession)
                    display("RAW Statistics", rawStatisticsPath, "key", sparkSession)
                }
                // get the typed version of the dataset, with columns and types specified in config
                val typedDataset = dataFrameBuilder.typed()
                display("Typed dataset", typedDataset, "date")

                // get the valid version of the dataset, de-duplicating if required
                val validDataset = dataFrameBuilder.valid(fileSource.table.deduplicate)
                display("Valid dataset", validDataset, "date")

                // statistics
                stats?.let {
                    val validStatisticsPath = "$outputPath/${fileSource.id}/valid"
                    generateStatistics(stats, validDataset, validStatisticsPath, sparkSession)
                    display("VALID Statistics", validStatisticsPath, "key", sparkSession)
                }
            }
        }

    }

    private fun generateStatistics(statisticConfiguration: Statistics, dataset: Dataset<Row>, path: String, sparkSession: SparkSession) {
        // transform from configuration to implementation
        val statistics = StatisticRepository().buildStatistics(statisticConfiguration)
        // instantiate a collector for gathering results
        val collector = SparkCollector(sparkSession, path)
        // process the statistics for the given dataset, and close the collector on completion
        // this will result in the statistics being written to CSV
        collector.use {
            StatisticsRunner().process(dataset, statistics, collector)
        }
    }

    private fun display(name: String, path: String, sort: String, sparkSession: SparkSession) {
        val statisticDataframe = sparkSession.read()
            .format("csv")
            .option("header", true)
            .load(path)

        display(name, statisticDataframe, sort)
    }

    private fun display(name: String, ds: Dataset<Row>, sort: String) {
        println()
        println(name)
        println()
        ds.printSchema()
        if (ds.columns().contains(sort)) {
            ds.orderBy(sort).show(displayRows)
        } else {
            ds.show(displayRows)
        }
        println("row count = ${ds.count()}")
    }
}

fun main() {
    println("Starting...")

    App().go()

    println("Finished...")
}
