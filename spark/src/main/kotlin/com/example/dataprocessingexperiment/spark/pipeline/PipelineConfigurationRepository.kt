package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.tables.pipeline.PipelineConfiguration
import io.github.xn32.json5k.Json5
import io.github.xn32.json5k.decodeFromStream
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.modules.SerializersModule
import org.json4s.Reader
import java.io.InputStream

// https://github.com/Kotlin/kotlinx.serialization/blob/master/docs/polymorphism.md
// https://stackoverflow.com/questions/68352475/best-way-to-deserialize-json-objects-based-in-property-value
// https://stackoverflow.com/questions/69847684/kotlinx-serialization-best-way-to-do-polymorphic-child-deserialization

/**
 * To set up the SerializersModule use
 *
 *         val module = SerializersModule {
 *             polymorphic(AbstractTask::class, JoinTask::class, JoinTask.serializer())
 *             polymorphic(AbstractTask::class, UnionTask::class, UnionTask.serializer())
 *             polymorphic(AbstractTask::class, LiteralTask::class, LiteralTask.serializer())
 *             ... add your custom tasks here
 *         }
 *
 */
class PipelineConfigurationRepository(
    val module: SerializersModule
) {
    val format = Json5 { serializersModule = module }

    fun load(inputStream: InputStream): PipelineConfiguration {
        return format.decodeFromStream<PipelineConfiguration>(inputStream)
    }
}