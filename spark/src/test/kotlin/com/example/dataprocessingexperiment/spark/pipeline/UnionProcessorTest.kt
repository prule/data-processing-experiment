package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
import com.example.dataprocessingexperiment.tables.pipeline.UnionTask
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

class UnionProcessorTest {

    @Test
    fun `should join tables`() {
        // prepare

        val data1 = listOf(
            GenericRow(arrayOf(1, "a")),
            GenericRow(arrayOf(2, "b")),
        )

        val data2 = listOf(
            GenericRow(arrayOf(3, "c")),
            GenericRow(arrayOf(4, "d")),
        )

        val dataframe1 = asDataFrame(data1)
        val dataframe2 = asDataFrame(data2)

        val context = SparkContext(Tables("test", "test", "test", listOf()))
        context.set("dataFrame1", dataframe1)
        context.set("dataFrame2", dataframe2)

        val task = UnionTask(
            "id",
            "name",
            "description",
            "unioned",
            listOf("dataFrame1","dataFrame2")
        )

        // perform
        UnionProcessor().process(context, task)

        // verify
        val result = context.get("unioned")

        result.count() shouldBe 4
        result.columns().size shouldBe 2
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