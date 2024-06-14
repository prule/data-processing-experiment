package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.data.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.pipeline.*
import com.example.dataprocessingexperiment.spark.statistics.StatisticRepository
import com.example.dataprocessingexperiment.spark.statistics.StatisticsRunner
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import com.example.dataprocessingexperiment.tables.Sources
import com.example.dataprocessingexperiment.tables.pipeline.ProcessorDefinition
import com.example.dataprocessingexperiment.tables.statistics.Statistics
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import io.github.xn32.json5k.decodeFromStream
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 * Reference implementation to demonstrate and exercise current capabilities.
 */

class App {
    private val displayRows = 100

    fun go() {
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // clean up the output directory
        val outputPath = "./build/output/sample1/statements/statistics"
        File(outputPath).deleteRecursively()

        // load configuration
        val defaultJsonSerializer = DefaultJsonSerializer()

        val sources = defaultJsonSerializer.tableModule().decodeFromStream<Sources>(
            this::class.java.getResourceAsStream("/sample1.tables.json5")!!
        )

        val statisticConfiguration = defaultJsonSerializer.statisticsModule().decodeFromStream<StatisticsConfiguration>(
            this::class.java.getResourceAsStream("/sample1.statistics.json5")!!
        )

        val context = SparkContext(sources)

        // run
        // load each table
        //  generate statistics if configured
        sparkSession.use {

            // populate context with tables
            sources.sources.forEach { source ->

                // set up the dataframe
                val dataFrameBuilder = DataFrameBuilder(
                    sparkSession,
                    source,
                    "../data/"
                )

                // ------------
                // RAW
                // ------------

                // get the raw version of the dataset, everything is a string, and all columns are included
                val rawDataset = dataFrameBuilder.raw
                display("Raw dataset", rawDataset, "date")

                // statistics
                val stats = statisticConfiguration.statisticsById(source.id)
                stats?.let {
                    val rawStatisticsPath = "$outputPath/${source.id}/raw"
                    generateStatistics(stats, rawDataset, rawStatisticsPath, sparkSession)
                    display("RAW Statistics", rawStatisticsPath, "key", sparkSession)
                }

                // ------------
                // SELECTED
                // ------------

                // get the selected version of the dataset, everything is a string, and only configured columns are included.
                // values will be trimmed if specified,
                // and columns will be aliased.
                val selectedDataset = dataFrameBuilder.selected()
                display("SELECTED dataset", selectedDataset, "date")

                // ------------
                // TYPED
                // ------------

                // get the typed version of the dataset, with columns and types specified in config
                val typedDataset = dataFrameBuilder.typed()
                display("Typed dataset", typedDataset, "date")

                // ------------
                // VALID
                // ------------

                // get the valid version of the dataset, de-duplicating if required
                val validDataset = dataFrameBuilder.valid(source.table.deduplicate)
                display("Valid dataset", validDataset, "date")

                // statistics
                stats?.let {
                    val validStatisticsPath = "$outputPath/${source.id}/valid"
                    generateStatistics(stats, validDataset, validStatisticsPath, sparkSession)
                    display("VALID Statistics", validStatisticsPath, "key", sparkSession)
                }

                // update the context
                context.set(source.id, validDataset)

            }

            val pipelineConfigurationRepository = PipelineConfigurationRepository(
                SerializersModule {
                    polymorphic(ProcessorDefinition::class, JoinProcessor::class, JoinProcessor.serializer())
                    polymorphic(ProcessorDefinition::class, UnionProcessor::class, UnionProcessor.serializer())
                    polymorphic(ProcessorDefinition::class, LiteralProcessor::class, LiteralProcessor.serializer())
                    polymorphic(ProcessorDefinition::class, OutputProcessor::class, OutputProcessor.serializer())
                    polymorphic(
                        ProcessorDefinition::class,
                        ValueMappingJoinProcessor::class,
                        ValueMappingJoinProcessor.serializer()
                    )
                    polymorphic(
                        ProcessorDefinition::class,
                        ValueMappingWhenProcessor::class,
                        ValueMappingWhenProcessor.serializer()
                    )
                }
            )

            val pipelineConfiguration = pipelineConfigurationRepository.load(
                File("./src/main/resources/sample1.pipeline.json5").inputStream()
            )

            PipelineProcessor(pipelineConfiguration).process(context)

            // display result
            context.show()
        }


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

    App().go()

    println("Finished...")
}
