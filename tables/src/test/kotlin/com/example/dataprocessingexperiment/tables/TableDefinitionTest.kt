package com.example.dataprocessingexperiment.tables

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TableDefinitionTest {
    @Test
    fun `should not trim when neither table or column specifies trim`() {
        val tableDefinition = TableDefinition(
            "test1",
            "test csv file",
            false,
            listOf(
                ColumnDefinition(listOf("boolean"), "boolean", "boolean", true, trim = false, type = SampleColType()),
            ),
            false,
        )

        tableDefinition.trim("boolean") shouldBe false
    }

    @Test
    fun `should trim when table specifies trim and column does not specify trim`() {
        val tableDefinition = TableDefinition(
            "test1",
            "test csv file",
            false,
            listOf(
                ColumnDefinition(listOf("boolean"), "boolean", "boolean", true, trim = null, type = SampleColType()),
            ),
            true,
        )

        tableDefinition.trim("boolean") shouldBe true
    }

    @Test
    fun `column trim should take precedence when table trim is false`() {
        val tableDefinition = TableDefinition(
            "test1",
            "test csv file",
            false,
            listOf(
                ColumnDefinition(listOf("boolean1"), "boolean", "boolean", true, trim = true, type = SampleColType()),
                ColumnDefinition(listOf("boolean2"), "boolean", "boolean", true, trim = false, type = SampleColType()),
            ),
            false,
        )

        tableDefinition.trim("boolean1") shouldBe true
        tableDefinition.trim("boolean2") shouldBe false
    }

    @Test
    fun `column trim should take precedence when table trim is true`() {
        val tableDefinition = TableDefinition(
            "test1",
            "test csv file",
            false,
            listOf(
                ColumnDefinition(listOf("boolean1"), "boolean", "boolean", true, trim = true, type = SampleColType()),
                ColumnDefinition(listOf("boolean2"), "boolean", "boolean", true, trim = false, type = SampleColType()),
            ),
            true,
        )

        tableDefinition.trim("boolean1") shouldBe true
        tableDefinition.trim("boolean2") shouldBe false
    }

}

class SampleColType : ColumnType