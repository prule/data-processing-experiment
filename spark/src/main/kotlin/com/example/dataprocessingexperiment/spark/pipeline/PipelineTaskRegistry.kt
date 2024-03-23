package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.tables.pipeline.*
import kotlin.reflect.KClass

class PipelineTaskRegistry {
    // map of provided tasks to processors
    private val taskMap = mapOf(
        JoinTaskDefinition::class to JoinProcessor::class,
        UnionTaskDefinition::class to UnionProcessor::class,
        LiteralTaskDefinition::class to LiteralProcessor::class,
        OutputTaskDefinition::class to OutputProcessor::class
    )

    private val externalTasks: MutableMap<KClass<out AbstractTaskDefinition>, KClass<out Processor>> = mutableMapOf()

    /**
     * Register a new processor for a given definition.
     */
    fun add(definition: KClass<out AbstractTaskDefinition>, processor: KClass<out Processor>) {
        externalTasks.put(definition, processor)
    }

    /**
     * Returns an instance of the Processor registered for the give task definition.
     */
    fun processor(id: KClass<AbstractTaskDefinition>): Processor {
        // gives preference to externally registered tasks so the defaults can be overridden
        val map = if (externalTasks.containsKey(id)) { externalTasks } else { taskMap }
        // instantiate the processor
        return map[id]!!.java.constructors.first { it.parameterCount == 0 }.newInstance() as Processor
    }
}