package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.data.DataFrameBuilder
import com.example.dataprocessingexperiment.spark.data.types.Types
import com.example.dataprocessingexperiment.spark.pipeline.*
import com.example.dataprocessingexperiment.spark.statistics.*
import com.example.dataprocessingexperiment.spark.statistics.collectors.SparkCollector
import com.example.dataprocessingexperiment.tables.Tables
import com.example.dataprocessingexperiment.tables.pipeline.*
import com.example.dataprocessingexperiment.tables.statistics.StatisticDefinition
import com.example.dataprocessingexperiment.tables.statistics.Statistics
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import io.github.xn32.json5k.Json5
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
        val outputPath = "./build/out/sample1/statements/statistics"
        File(outputPath).deleteRecursively()

        // load configuration
        val tables = Json5.decodeFromStream<Tables>(
            this::class.java.getResourceAsStream("/sample1.tables.json5")!!
        )

        val module = SerializersModule {
            polymorphic(StatisticDefinition::class, Bounds::class, Bounds.serializer())
            polymorphic(StatisticDefinition::class, ColCount::class, ColCount.serializer())
            polymorphic(StatisticDefinition::class, CountByMonth::class, CountByMonth.serializer())
            polymorphic(StatisticDefinition::class, CountByValue::class, CountByValue.serializer())
            polymorphic(StatisticDefinition::class, DuplicateCount::class, DuplicateCount.serializer())
            polymorphic(StatisticDefinition::class, Maximum::class, Maximum.serializer())
            polymorphic(StatisticDefinition::class, Minimum::class, Minimum.serializer())
            polymorphic(StatisticDefinition::class, RowCount::class, RowCount.serializer())
            polymorphic(StatisticDefinition::class, EmptyCount::class, EmptyCount.serializer())
            polymorphic(StatisticDefinition::class, Summary::class, Summary.serializer())
        }

        val format = Json5 { serializersModule = module }

        val statisticConfiguration = format.decodeFromStream<StatisticsConfiguration>(
            this::class.java.getResourceAsStream("/sample1.statistics.json5")!!
        )

        val context = SparkContext(tables)

        // run
        // load each table
        //  generate statistics if configured
        sparkSession.use {

            // populate context with tables
            tables.sources.forEach { source ->

                // set up the dataframe
                val dataFrameBuilder = DataFrameBuilder(
                    sparkSession,
                    source,
                    Types.all(),
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

                // get the raw version of the dataset, everything is a string, and all columns are included
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
                    polymorphic(ProcessorDefinition::class, ValueMappingJoinProcessor::class, ValueMappingJoinProcessor.serializer())
                    polymorphic(ProcessorDefinition::class, ValueMappingWhenProcessor::class, ValueMappingWhenProcessor.serializer())
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
