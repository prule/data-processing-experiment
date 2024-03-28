package com.example.dataprocessingexperiment.spark.pipeline

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.tables.Tables
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class ValueMappingJoinProcessorTest {
    private val dataHelper = SparkDataHelper(sparkSession, true)

    @Test
    fun `should substitute values`() {
        // prepare
        val tableName = "transactions"
        val columnName = "description"
        
        // mappings
        // converts a description of "burgers" to "burger" in the transactions table

        //        +------------+-----------+-------+------+
        //        |       table|     column|   from|    to|
        //        +------------+-----------+-------+------+
        //        |transactions|description|burgers|burger|
        //        +------------+-----------+-------+------+

        val dataframe1 = dataHelper.asDataFrame(
            listOf(
                GenericRow(arrayOf(tableName, columnName, "burgers", "burger")),
            ),
            listOf(
                Pair("table", DataTypes.StringType),
                Pair("column", DataTypes.StringType),
                Pair("from", DataTypes.StringType),
                Pair("to", DataTypes.StringType),
            )
        )

        // transactions

        //        +----+-----------+----+
        //        |val1|description|val2|
        //        +----+-----------+----+
        //        |   a|    burgers|   1|
        //        |   b|     burger|   2|
        //        |   c|      apple|   3|
        //        |   d|       NULL|   4|
        //        +----+-----------+----+

        val dataframe2 = dataHelper.asDataFrame(
            listOf(
                GenericRow(arrayOf("a", "burgers", "1")),
                GenericRow(arrayOf("b", "burger", "2")),
                GenericRow(arrayOf("c", "apple", "3")),
                GenericRow(arrayOf("d", null, "4")),
            ),
            listOf(
                Pair("val1", DataTypes.StringType),
                Pair(columnName, DataTypes.StringType),
                Pair("val2", DataTypes.StringType),
            )
        )

        // build context
        val context = SparkContext(Tables("test", "test", "test", listOf()))
        context.set("mappings", dataframe1)
        context.set(tableName, dataframe2)

        // perform
        ValueMappingJoinProcessor(
            "id",
            "name",
            columnName,
            listOf("mappings"),
        ).process(context)

        // expect result
        //        +----+-----------+----+
        //        |val1|description|val2|
        //        +----+-----------+----+
        //        |   a|     burger|   1|
        //        |   b|     burger|   2|
        //        |   c|      apple|   3|
        //        |   d|       NULL|   4|
        //        +----+-----------+----+

        val expected = dataHelper.asDataFrame(
            listOf(
                GenericRow(arrayOf("a", "burger", "1")),
                GenericRow(arrayOf("b", "burger", "2")),
                GenericRow(arrayOf("c", "apple", "3")),
                GenericRow(arrayOf("d", null, "4")),
            ), listOf(
                Pair("val1", DataTypes.StringType),
                Pair(columnName, DataTypes.StringType),
                Pair("val2", DataTypes.StringType),
            )
        )

        // verify
        context.get(tableName).except(expected).count() shouldBe 0
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