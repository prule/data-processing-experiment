package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.tables.pipeline.AbstractTask
import com.example.dataprocessingexperiment.tables.pipeline.JoinTask
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTask
import com.example.dataprocessingexperiment.tables.pipeline.UnionTask
import io.kotest.matchers.shouldBe
import kotlinx.serialization.modules.SerializersModule
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.File

class PipelineConfigurationRepositoryTest {

    @Test
    fun `should load configuration`() {
        val pipelineConfigurationRepository = PipelineConfigurationRepository(
            SerializersModule {
                polymorphic(AbstractTask::class, JoinTask::class, JoinTask.serializer())
                polymorphic(AbstractTask::class, UnionTask::class, UnionTask.serializer())
                polymorphic(AbstractTask::class, LiteralTask::class, LiteralTask.serializer())
            }
        )

        val pipelineConfiguration = pipelineConfigurationRepository.load(
            File("./src/test/resources/sample1.pipeline.json5").inputStream()
        )

        pipelineConfiguration.tasks[0].javaClass shouldBe JoinTask::class.java
        pipelineConfiguration.tasks[1].javaClass shouldBe UnionTask::class.java
        pipelineConfiguration.tasks[2].javaClass shouldBe LiteralTask::class.java
    }
}