package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.ProcessorDefinition

interface Processor: ProcessorDefinition {
    fun process(context: SparkContext)

}