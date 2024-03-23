package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
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
    private val dataHelper = SparkDataHelper(sparkSession)

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

        val dataframe1 = dataHelper.asDataFrame(
            data1, listOf(
                Pair("val1", DataTypes.IntegerType),
                Pair("val2", DataTypes.StringType),
            )
        )

        val dataframe2 = dataHelper.asDataFrame(
            data2, listOf(
                Pair("val1", DataTypes.IntegerType),
                Pair("val2", DataTypes.StringType),
            )
        )

        val context = SparkContext(Tables("test", "test", "test", listOf()))
        context.set("dataFrame1", dataframe1)
        context.set("dataFrame2", dataframe2)

        // perform
        UnionProcessor(
            "id",
            "name",
            "description",
            "unioned",
            listOf("dataFrame1","dataFrame2")
        ).process(context)

        // verify
        val result = context.get("unioned")

        result.count() shouldBe 4
        result.columns().size shouldBe 2
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