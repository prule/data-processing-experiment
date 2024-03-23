package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.AbstractTaskDefinition

interface Processor {
    fun process(context: SparkContext, task: AbstractTaskDefinition)

}