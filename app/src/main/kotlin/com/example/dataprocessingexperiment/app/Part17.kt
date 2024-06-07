package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.data.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.statistics.StatisticRepository
import com.example.dataprocessingexperiment.spark.statistics.StatisticsRunner
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import com.example.dataprocessingexperiment.tables.Sources
import com.example.dataprocessingexperiment.tables.statistics.Statistics
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import io.github.xn32.json5k.decodeFromStream
import mu.KotlinLogging
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 * Reference implementation to demonstrate and exercise current capabilities.
 */

class Part17 {
    private val displayRows = 100
    private val logger = KotlinLogging.logger {}

    fun go() {
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // clean up the output directory
        val outputPath = "./build/out/part17/statistics"
        File(outputPath).deleteRecursively()

        // load configuration
        val defaultJsonSerializer = DefaultJsonSerializer()

        val sources = defaultJsonSerializer.tableModule().decodeFromStream<Sources>(
            File("./data/part17/part17.tables.json5").inputStream()
        )

        val statisticConfiguration = defaultJsonSerializer.statisticsModule().decodeFromStream<StatisticsConfiguration>(
            File("./data/part17/part17.statistics.json5").inputStream()
        )

        val context = SparkContext(sources)

        val stopWatch = StopWatch.createStarted()

        // run
        // load each table
        //  generate statistics if configured
        sparkSession.use {

            // populate context with tables
            sources.sources.forEach { source ->

                val stopWatchSource = StopWatch.createStarted()

                // set up the dataframe
                val dataFrameBuilder = DataFrameBuilder(
                    sparkSession,
                    source,
                    "./data/part17/"
                )

                // ------------
                // RAW
                // ------------

                // get the raw version of the dataset, everything is a string, and all columns are included
                val rawDataset = dataFrameBuilder.raw.persist()
//                display("Raw dataset", rawDataset, "date")

                // statistics
                val stats = statisticConfiguration.statisticsById(source.id)
                stats?.let {
                    val rawStatisticsPath = "$outputPath/${source.id}/raw"
                    generateStatistics(stats, rawDataset, rawStatisticsPath, sparkSession)
//                    display("RAW Statistics", rawStatisticsPath, "key", sparkSession)
                }

                // ------------
                // SELECTED
                // ------------

                // get the selected version of the dataset, everything is a string, and only configured columns are included.
                // values will be trimmed if specified,
                // and columns will be aliased.
//                val selectedDataset = dataFrameBuilder.selected()
//                display("SELECTED dataset", selectedDataset, "date")

                // ------------
                // TYPED
                // ------------

                // get the typed version of the dataset, with columns and types specified in config
//                val typedDataset = dataFrameBuilder.typed()
//                display("Typed dataset", typedDataset, "date")

                // ------------
                // VALID
                // ------------

                // get the valid version of the dataset, de-duplicating if required
                val validDataset = dataFrameBuilder.valid(source.table.deduplicate)
//                display("Valid dataset", validDataset, "date")

                // statistics
                stats?.let {
                    val validStatisticsPath = "$outputPath/${source.id}/valid"
                    generateStatistics(stats, validDataset, validStatisticsPath, sparkSession)
//                    display("VALID Statistics", validStatisticsPath, "key", sparkSession)
                }

                // update the context
                context.set(source.id, validDataset)

                logger.info { "${source.id} took $stopWatchSource" }
            }

        }
        logger.info { "Took $stopWatch" }

    }

    private fun generateStatistics(
        statisticConfiguration: Statistics,
        dataset: Dataset<Row>,
        path: String,
        sparkSession: SparkSession
    ) {
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
            ds.orderBy(sort).show(displayRows, 10)
        } else {
            ds.show(displayRows, 10)
        }
        println("row count = ${ds.count()}")
    }
}

fun main() {
    println("Starting...")

    Part17().go()

    println("Finished...")
}

// With original code, it took 27 seconds and FileScanRDD occurred 40 times.
// 24/06/07 20:39:24 INFO Part17: Took 00:00:27.173
//
// Removing calls to display dataframes, it took 22 seconds and FileScanRDD occurred 18 times.
// 24/06/07 20:43:59 INFO Part17: Took 00:00:22.464
//
// Using persist on the raw dataset, it took 9 seconds and FileScanRDD occurred 3 times.
// 24/06/07 20:52:49 INFO Part17: Took 00:00:09.479
// addresses is read twice
// cities is read once
//
