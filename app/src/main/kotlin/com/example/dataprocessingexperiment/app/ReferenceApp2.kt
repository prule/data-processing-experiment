package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.DatasetOutput
import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.StatisticProcessor
import com.example.dataprocessingexperiment.spark.TableProcessor
import com.example.dataprocessingexperiment.spark.pipeline.*
import com.example.dataprocessingexperiment.tables.Sources
import com.example.dataprocessingexperiment.tables.pipeline.PipelineConfiguration
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import io.github.xn32.json5k.decodeFromStream
import mu.KotlinLogging
import org.apache.commons.lang3.time.StopWatch
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 * A reference application implementing the transforms described in Chapter 2 of "Solve any data analysis problem"
 * https://github.com/davidasboth/solve-any-data-analysis-problem/blob/main/chapter-2/Chapter%202%20sample%20solution.ipynb
 *
 * The experiment here is to consider:
 * 1. Use a notebook to investigate the data and figure out what the requirement for data transform would be
 * 2. Configure the framework to implement this transformation
 * 3. Use a notebook to report and graph using that transformed data
 *
 * This might be useful in the case that the data would be updated regularly and the transform needed to be run regularly.
 * Sure, you could just do it all in a notebook, but this is a test to see how this framework could be applied to such a problem and what that would look like.
 *
 */

class ReferenceApp2 {
    private val logger = KotlinLogging.logger {}
    private val outputBase = "./build/output/reference-app-2"
    private val dataBase = "../data/reference-app-2"

    fun go() {
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // clean up the output directory
        val outputPath = "$outputBase/statistics"
        File(outputPath).deleteRecursively()

        // load configuration
        val defaultJsonSerializer = DefaultJsonSerializer()

        val sources = defaultJsonSerializer.tableModule().decodeFromStream<Sources>(
            File("$dataBase/reference-app-2.tables.json5").inputStream()
        )

        val statisticConfiguration = defaultJsonSerializer.statisticsModule().decodeFromStream<StatisticsConfiguration>(
            File("$dataBase/reference-app-2.statistics.json5").inputStream()
        )

        val pipelineConfiguration = defaultJsonSerializer.pipelineModule().decodeFromStream<PipelineConfiguration>(
            File("$dataBase/reference-app-2.pipeline.json5").inputStream()
        )

        val datasetOutput = DatasetOutput()
        val context = SparkContext(sources, sparkSession, datasetOutput)
        val stopWatch = StopWatch.createStarted()

        // run
        sparkSession.use {

            val statisticProcessor = StatisticProcessor(sparkSession)

            TableProcessor(
                sources,
                statisticConfiguration,
                dataBase,
                outputPath,
                statisticProcessor,
                datasetOutput
            ).process(context)

            PipelineProcessor(pipelineConfiguration).process(context)

            // display result
            context.show()

            context.tablesIds().forEach { id ->
                // statistics
                logger.info { "transformed stats $id" }
                statisticProcessor.process(
                    statisticConfiguration.statisticsById(id),
                    context.get(id),
                    "$outputPath/${id}/transformed"
                )

            }

        }
        logger.info { "Took $stopWatch" }

    }

//    private fun generateStatistics(
//        statisticConfiguration: Statistics,
//        dataset: Dataset<Row>,
//        path: String,
//        sparkSession: SparkSession
//    ) {
//        // transform from configuration to implementation
//        val statistics = StatisticRepository().buildStatistics(statisticConfiguration)
//        // instantiate a collector for gathering results
//        val collector = SparkCollector(sparkSession, path)
//        // process the statistics for the given dataset, and close the collector on completion
//        // this will result in the statistics being written to CSV
//        collector.use {
//            StatisticsRunner().process(dataset, statistics, collector)
//        }
//    }

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
