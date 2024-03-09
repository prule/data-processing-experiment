package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.tables.pipeline.AbstractTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.JoinTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.UnionTaskDefinition
import io.kotest.matchers.shouldBe
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Test
import java.io.File

class PipelineConfigurationRepositoryTest {

    @Test
    fun `should load configuration`() {
        val pipelineConfigurationRepository = PipelineConfigurationRepository(
            SerializersModule {
                polymorphic(AbstractTaskDefinition::class, JoinTaskDefinition::class, JoinTaskDefinition.serializer())
                polymorphic(AbstractTaskDefinition::class, UnionTaskDefinition::class, UnionTaskDefinition.serializer())
                polymorphic(AbstractTaskDefinition::class, LiteralTaskDefinition::class, LiteralTaskDefinition.serializer())
            }
        )

        val pipelineConfiguration = pipelineConfigurationRepository.load(
            File("./src/test/resources/sample1.pipeline.json5").inputStream()
        )

        pipelineConfiguration.tasks[0].javaClass shouldBe JoinTaskDefinition::class.java
        pipelineConfiguration.tasks[1].javaClass shouldBe UnionTaskDefinition::class.java
        pipelineConfiguration.tasks[2].javaClass shouldBe LiteralTaskDefinition::class.java
    }
}