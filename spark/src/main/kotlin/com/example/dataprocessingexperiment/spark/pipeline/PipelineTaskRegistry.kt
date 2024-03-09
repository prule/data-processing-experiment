package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.tables.pipeline.AbstractTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.JoinTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.UnionTaskDefinition
import kotlin.reflect.KClass

class PipelineTaskRegistry {
    private val taskMap = mapOf(
        JoinTaskDefinition::class to JoinProcessor::class,
        UnionTaskDefinition::class to UnionProcessor::class,
        LiteralTaskDefinition::class to LiteralProcessor::class
    )

    private val externalTasks: MutableMap<KClass<out AbstractTaskDefinition>, KClass<out Processor>> = mutableMapOf()

    fun processor(id: KClass<AbstractTaskDefinition>): Processor {
        val map = if (externalTasks.containsKey(id)) {
            externalTasks
        } else {
            taskMap
        }

        return map[id]!!.java.constructors.first { it.parameterCount == 0 }.newInstance() as Processor
    }
}