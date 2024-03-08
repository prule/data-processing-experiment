package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTask
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.collections.shouldContainOnly
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class LiteralProcessorTest {

    @Test
    fun `should add literal columns`() {
        val data1 = listOf(
            GenericRow(arrayOf(-1, "a")),
            GenericRow(arrayOf(10, "b")),
            GenericRow(arrayOf(5, "c")),
            GenericRow(arrayOf(null, "d")),
        )

        val dataframe1 = asDataFrame(data1)

        val context = SparkContext(Tables("test", "test", "test", listOf()))
        context.set("dataFrame1", dataframe1)

        val literalTask = LiteralTask(
//            "com.example.dataprocessingexperiment.tables.pipeline.LiteralTask",
            "id",
            "name",
            "description",
            "dataFrame1",
            mapOf("a" to "1", "b" to "2"),
        )

        LiteralProcessor().process(context, literalTask)

        val result = context.get("dataFrame1")
        result.count() shouldBe 4
        result.columns().size shouldBe 4
        result.columns() shouldContainAll (listOf(
            "val1",
            "val2",
            "a",
            "b"
        ))
        result.select("a").distinct().collectAsList() shouldContainOnly listOf("1")
        result.select("b").distinct().collectAsList() shouldContainOnly listOf("1")
    }

    private fun asDataFrame(data: List<GenericRow>): Dataset<Row> {
        return sparkSession.createDataFrame(
            data, StructType(
                arrayOf(
                    StructField("val1", DataTypes.IntegerType, false, Metadata.empty()),
                    StructField("val2", DataTypes.StringType, false, Metadata.empty())
                )
            )
        )
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