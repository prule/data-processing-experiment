package com.example.dataprocessingexperiment.tables

import io.github.xn32.json5k.Json5
import io.github.xn32.json5k.decodeFromStream
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldNotBe
import kotlin.test.Test

class FileSourceTest {
    @Test
    fun file_should_load() {
        val fileSource = Json5.decodeFromStream<FileSource>(
            this::class.java.getResourceAsStream("/tables/file-source-1.json5")!!
        )

        fileSource.table shouldNotBe null
        fileSource.table.columns shouldNotBe null
        fileSource.table.columns.size shouldBeExactly 4
    }
}