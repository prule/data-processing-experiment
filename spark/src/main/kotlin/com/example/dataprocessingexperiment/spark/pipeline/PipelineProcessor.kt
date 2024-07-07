package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.PipelineConfiguration
import mu.KotlinLogging

class PipelineProcessor(
    private val configuration: PipelineConfiguration
) {
    private val logger = KotlinLogging.logger {}

    /**
     * Process the configuration using the supplied context.
     */
    fun process(context: SparkContext) {
        logger.info { "Starting pipeline id=${configuration.id} name=${configuration.name}" }
        configuration.tasks.forEach { processor ->
            logger.info { "Starting processor $processor" }
            (processor as Processor).process(context)
            logger.info { "Finished processor $processor" }
        }
    }
}