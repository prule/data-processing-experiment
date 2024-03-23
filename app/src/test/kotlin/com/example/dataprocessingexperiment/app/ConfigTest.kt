package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.spark.pipeline.JoinProcessor
import com.example.dataprocessingexperiment.spark.pipeline.LiteralProcessor
import com.example.dataprocessingexperiment.spark.pipeline.OutputProcessor
import com.example.dataprocessingexperiment.spark.pipeline.UnionProcessor
import com.example.dataprocessingexperiment.tables.pipeline.*
import io.github.xn32.json5k.Json5
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Test

class ConfigTest {

    @Test
    fun `should serialize`() {

        val module = SerializersModule {
            polymorphic(ProcessorDefinition::class, JoinProcessor::class, JoinProcessor.serializer())
            polymorphic(ProcessorDefinition::class, UnionProcessor::class, UnionProcessor.serializer())
            polymorphic(ProcessorDefinition::class, LiteralProcessor::class, LiteralProcessor.serializer())
            polymorphic(ProcessorDefinition::class, OutputProcessor::class, OutputProcessor.serializer())
        }
        val format = Json5 { serializersModule = module }

        val joinTask: ProcessorDefinition = JoinProcessor(
//            "com.example.dataprocessingexperiment.tables.pipeline.JoinTask",
            "id",
            "name",
            "description",
            "from",
            "to",
            "destination",
            "inner",
            mapOf("a" to "1", "b" to "2"),
            listOf("z")
        )
        val string = format.encodeToString<ProcessorDefinition>(joinTask)
        println(string)
        val joinTask1 = format.decodeFromString<ProcessorDefinition>(string)

        println(joinTask1)

    }

    @Test
    fun `should serialize pipeline`() {

        val module = SerializersModule {
            polymorphic(ProcessorDefinition::class, JoinProcessor::class, JoinProcessor.serializer())
            polymorphic(ProcessorDefinition::class, UnionProcessor::class, UnionProcessor.serializer())
            polymorphic(ProcessorDefinition::class, LiteralProcessor::class, LiteralProcessor.serializer())
            polymorphic(ProcessorDefinition::class, OutputProcessor::class, OutputProcessor.serializer())
        }

        val joinTask: ProcessorDefinition = JoinProcessor(
            "id",
            "name",
            "description",
            "from",
            "to",
            "joinedTable",
            "inner",
            mapOf("a" to "1", "b" to "2"),
            listOf("z")
        )

        val unionTask: ProcessorDefinition = UnionProcessor(
            "id",
            "name",
            "description",
            "xyz",
            listOf("a", "b")
        )

        val literalTask: ProcessorDefinition = LiteralProcessor(
            "id",
            "name",
            "description",
            "xyz",
            mapOf("a" to "1", "b" to "2"),
        )

        val pipelineConfiguration = PipelineConfiguration(
            "id",
            "name",
            "description",
            listOf(joinTask, unionTask, literalTask)
        )

        val format = Json5 { serializersModule = module }

        val string = format.encodeToString<PipelineConfiguration>(pipelineConfiguration)
        println(string)
        val pipeline = format.decodeFromString<PipelineConfiguration>(string)

        println(pipeline)

    }

}