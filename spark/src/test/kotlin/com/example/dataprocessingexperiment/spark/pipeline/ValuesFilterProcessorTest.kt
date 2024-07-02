package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Sources
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class ValuesFilterProcessorTest {
    private val dataHelper = SparkDataHelper(sparkSession)

    fun setupContext(): SparkContext {
        val data1 = listOf(
            GenericRow(arrayOf(-1, "a")),
            GenericRow(arrayOf(10, "b")),
            GenericRow(arrayOf(5, "c")),
            GenericRow(arrayOf(null, "d")),
        )

        val dataframe1 = dataHelper.asDataFrame(
            data1, listOf(
                Pair("val1", DataTypes.IntegerType),
                Pair("val2", DataTypes.StringType),
            )
        )

        val context = SparkContext(Sources("test", "test", "test", listOf()), sparkSession)
        context.set("dataFrame1", dataframe1)
        return context
    }

    @Test
    fun `should exclude rows with matching values`() {
        // setup
        val context = setupContext()

        // perform
        ValuesFilterProcessor(
            "id",
            "name",
            "description",
            "dataFrame1",
            "val2",
            listOf("a", "b"),
            true
        ).process(context)

        // verify
        val result = context.get("dataFrame1")
        result.count() shouldBe 2
        result.select("val2")
            .collectAsList()
            .map { row -> row.getString(0) } shouldBe listOf("c", "d")
    }

    @Test
    fun `should include rows with matching values`() {
        // setup
        val context = setupContext()

        // perform
        ValuesFilterProcessor(
            "id",
            "name",
            "description",
            "dataFrame1",
            "val2",
            listOf("a", "b"),
            false
        ).process(context)

        // verify
        val result = context.get("dataFrame1")
        result.count() shouldBe 2
        result.select("val2")
            .collectAsList()
            .map { row -> row.getString(0) } shouldBe listOf("a", "b")
    }

    companion object {
        private val sparkSessionHelper = SparkSessionHelper()
        private val sparkSession: SparkSession by lazy { sparkSessionHelper.open() }

        @JvmStatic
        @AfterAll
        fun after() {
            sparkSessionHelper.close()
        }
    }
}