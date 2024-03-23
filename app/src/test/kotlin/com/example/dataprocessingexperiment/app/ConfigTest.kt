package com.example.dataprocessingexperiment.app

import com.example.dataprocessingexperiment.tables.pipeline.*
import io.github.xn32.json5k.Json5
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Test

class ConfigTest {

    @Test
    fun `should work`() {

        val module = SerializersModule {
            polymorphic(AbstractTaskDefinition::class, JoinTaskDefinition::class, JoinTaskDefinition.serializer())
            polymorphic(AbstractTaskDefinition::class, UnionTaskDefinition::class, UnionTaskDefinition.serializer())
            polymorphic(AbstractTaskDefinition::class, LiteralTaskDefinition::class, LiteralTaskDefinition.serializer())
        }
        val format = Json5 { serializersModule = module }

        val joinTask: AbstractTaskDefinition = JoinTaskDefinition(
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
        val string = format.encodeToString<AbstractTaskDefinition>(joinTask)
        println(string)
        val joinTask1 = format.decodeFromString<AbstractTaskDefinition>(string)

        println(joinTask1)

    }

    @Test
    fun `should work too`() {
//        val module = SerializersModule {
//            listOf(JoinTask::class,UnionTask::class).forEach {
//                val method = it.members.find { it2-> it2.name == "serializer" }!!
//                val result = method.call()
//                polymorphic(AbstractTask::class, it, it.serializer())
//            }
//            polymorphic(AbstractTask::class, JoinTask::class, JoinTask.serializer())
//            polymorphic(AbstractTask::class, UnionTask::class, UnionTask.serializer())
//        }
//
//        val map: Map<T: AbstractTask, KSerializer<T>> = mapOf(
//            JoinTask::class to JoinTask.serializer(),
//            UnionTask::class to UnionTask.serializer(),
//            LiteralTask::class to LiteralTask.serializer()
//        )

        val module = SerializersModule {
//            map.forEach { entry ->
//                polymorphic(AbstractTask::class, JoinTask::class, entry.value as KSerializer<out AbstractTask>)
//            }

            polymorphic(AbstractTaskDefinition::class, JoinTaskDefinition::class, JoinTaskDefinition.serializer())
            polymorphic(AbstractTaskDefinition::class, UnionTaskDefinition::class, UnionTaskDefinition.serializer())
            polymorphic(AbstractTaskDefinition::class, LiteralTaskDefinition::class, LiteralTaskDefinition.serializer())
        }

//        SerializersModuleBuilder().polymorphic(AbstractTask::class)


        val joinTask: AbstractTaskDefinition = JoinTaskDefinition(
//            "com.example.dataprocessingexperiment.tables.pipeline.JoinTask",
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

        val unionTask: AbstractTaskDefinition = UnionTaskDefinition(
//            "com.example.dataprocessingexperiment.tables.pipeline.UnionTask",
            "id",
            "name",
            "description",
            "xyz",
            listOf("a", "b")
        )

        val literalTask: AbstractTaskDefinition = LiteralTaskDefinition(
//            "com.example.dataprocessingexperiment.tables.pipeline.LiteralTask",
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