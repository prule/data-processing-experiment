package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.tables.pipeline.AbstractTask
import com.example.dataprocessingexperiment.tables.pipeline.JoinTask
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTask
import com.example.dataprocessingexperiment.tables.pipeline.UnionTask
import kotlin.reflect.KClass

class PipelineTaskRegistry {
    private val taskMap = mapOf(
        JoinTask::class to JoinProcessor::class,
        UnionTask::class to UnionProcessor::class,
        LiteralTask::class to LiteralProcessor::class
    )

    private val externalTasks: MutableMap<KClass<out AbstractTask>, KClass<out Processor>> = mutableMapOf()

    fun processor(id: KClass<AbstractTask>): Processor {
        val map = if (externalTasks.containsKey(id)) {
            externalTasks
        } else {
            taskMap
        }

        return map[id]!!.java.constructors.first { it.parameterCount == 0 }.newInstance() as Processor
    }
}