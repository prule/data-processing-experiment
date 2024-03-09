package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
import com.example.dataprocessingexperiment.tables.pipeline.JoinTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.LiteralTaskDefinition
import com.example.dataprocessingexperiment.tables.pipeline.PipelineConfiguration
import io.kotest.matchers.collections.shouldContainAll
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

class PipelineProcessorTest {

    val data1 = listOf(
        GenericRow(arrayOf(-1, "a")),
        GenericRow(arrayOf(10, "b")),
        GenericRow(arrayOf(5, "c")),
        GenericRow(arrayOf(null, "d")),
    )

    val data2 = listOf(
        GenericRow(arrayOf(-1, "a")),
        GenericRow(arrayOf(15, "b")),
        GenericRow(arrayOf(5, "c")),
        GenericRow(arrayOf(null, "d")),
    )

    val dataframe1 = asDataFrame(data1)
    val dataframe2 = asDataFrame(data2)

    @Test
    fun `should process pipeline`() {

        // prepare

        val context = SparkContext(Tables("test", "test", "test", listOf()))
        context.set("dataFrame1", dataframe1)
        context.set("dataFrame2", dataframe2)

        val joinTask = JoinTaskDefinition(
            "id",
            "name",
            "description",
            "dataFrame1",
            "dataFrame2",
            "joinedTable",
            mapOf("val1" to "val1"),
            listOf("val1")
        )

        val literalTask = LiteralTaskDefinition(
            "id",
            "name",
            "description",
            "dataFrame1",
            mapOf("a" to "1", "b" to "2"),
        )

        val pipelineProcessor = PipelineProcessor(
            PipelineConfiguration(
                "id",
                "name",
                "description",
                listOf(joinTask, literalTask)
            )
        )

        // perform

        pipelineProcessor.process(context)

        // verify

        val result1 = context.get("joinedTable")

        result1.count() shouldBe 3
        result1.columns().size shouldBe 3
        result1.columns() shouldContainAll (listOf("val1", "val2", "dataFrame2_val1"))

        val result2 = context.get("dataFrame1")
        result2.columns() shouldContainAll (listOf("val1", "val2", "a", "b"))
        result2.show()

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