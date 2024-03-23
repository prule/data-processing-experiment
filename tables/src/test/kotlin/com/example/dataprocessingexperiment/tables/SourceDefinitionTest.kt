package com.example.dataprocessingexperiment.tables

import io.github.xn32.json5k.Json5
import io.github.xn32.json5k.decodeFromStream
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldNotBe
import kotlin.test.Test

class SourceDefinitionTest {
    @Test
    fun file_should_load() {
        val sourceDefinition = Json5.decodeFromStream<SourceDefinition>(
            this::class.java.getResourceAsStream("/tables/file-source-1.json5")!!
        )

        sourceDefinition.table shouldNotBe null
        sourceDefinition.table.columns shouldNotBe null
        sourceDefinition.table.columns.size shouldBeExactly 4
    }
}