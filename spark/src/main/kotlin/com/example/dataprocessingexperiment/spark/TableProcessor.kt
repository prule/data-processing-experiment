package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.spark.data.DataFrameBuilder
import com.example.dataprocessingexperiment.tables.Sources
import com.example.dataprocessingexperiment.tables.statistics.StatisticsConfiguration
import mu.KotlinLogging
import org.apache.commons.lang3.time.StopWatch

class TableProcessor(
    val sources: Sources,
    private val statisticConfiguration: StatisticsConfiguration,
    private val dataBase: String,
    private val outputPath: String,
    private val statisticProcessor: StatisticProcessor,
    private val datasetOutput: DatasetOutput,
) {
    private val logger = KotlinLogging.logger {}

    fun process(context: SparkContext) {
        // populate context with tables
        val sparkSession = context.sparkSession

        sources.sources.forEach { source ->

            val stopWatchSource = StopWatch.createStarted()

            // set up the dataframe
            val dataFrameBuilder = DataFrameBuilder(
                sparkSession,
                source,
                "$dataBase/"
            )

            val stats = statisticConfiguration.statisticsById(source.id)

            // raw
            logger.info { "raw ${source.id}" }
            val rawDataset = dataFrameBuilder.raw.persist()
            datasetOutput.show(rawDataset)
            statisticProcessor.process(stats, rawDataset, "$outputPath/${source.id}/raw")

            // valid
            logger.info { "valid ${source.id}" }
            val validDataset = dataFrameBuilder.valid(source.table.deduplicate)
            datasetOutput.show(validDataset)
            statisticProcessor.process(stats, validDataset, "$outputPath/${source.id}/valid")

            logger.info { "${source.id} took $stopWatchSource" }

            // update the context
            context.set(source.id, validDataset)

        }

    }

}