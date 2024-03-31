package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Sources
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class LiteralProcessorTest {
    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should add literal columns`() {
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

        val context = SparkContext(Sources("test", "test", "test", listOf()))
        context.set("dataFrame1", dataframe1)

        LiteralProcessor(//            "com.example.dataprocessingexperiment.tables.pipeline.LiteralTask",
            "id",
            "name",
            "description",
            "dataFrame1",
            mapOf("a" to "1", "b" to "2"),).process(context)

        val result = context.get("dataFrame1")
        result.count() shouldBe 4
        result.columns().size shouldBe 4
        result.columns() shouldContainAll (listOf(
            "val1",
            "val2",
            "a",
            "b"
        ))
        result.select("a").distinct().collectAsList()[0][0] shouldBe "1"
        result.select("b").distinct().collectAsList()[0][0] shouldBe "2"
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