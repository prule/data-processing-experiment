package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.PipelineConfiguration
import mu.KotlinLogging

class PipelineProcessor(private val configuration: PipelineConfiguration) {
    private val logger = KotlinLogging.logger {}

    private val taskRegistry = PipelineTaskRegistry()
    fun process(context: SparkContext) {
        logger.info { "Starting pipeline id=${configuration.id} name=${configuration.name}" }
        configuration.tasks.forEach { task ->
            logger.info { "Applying task id=${task.id} name=${task.name}"}
            val processor = taskRegistry.processor(task.javaClass.kotlin)
            logger.info { "Starting processor $processor" }
            processor.process(context, task)
            logger.info { "Finished processor $processor" }
        }
    }
}