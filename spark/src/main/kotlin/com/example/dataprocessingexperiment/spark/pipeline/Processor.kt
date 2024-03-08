package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.tables.pipeline.AbstractTask

interface Processor {
    fun process(context: SparkContext, task: AbstractTask)

}