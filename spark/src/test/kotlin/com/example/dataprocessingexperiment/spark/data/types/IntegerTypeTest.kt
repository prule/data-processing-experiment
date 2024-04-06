package com.example.dataprocessingexperiment.spark.data.types

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class IntegerTypeTest {
    private val columnName = "value"
    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should convert valid numbers`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("1")),
            GenericRow(arrayOf("2")),
            GenericRow(arrayOf("3.1")),
            GenericRow(arrayOf("4.6")),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val column = IntegerType().process(columnName)

        // perform
        val result = dataframe.select(column).collectAsList().map { it.get(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            1,
            2,
            3,
            4
        ))
    }

    @Test
    fun `invalid numbers should be null`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("zzz")),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val column = IntegerType().process(columnName)

        // perform
        val result = dataframe.select(column).collectAsList().map { it.get(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            null,
        ))
    }

    companion object {
        private val sparkSessionHelper = SparkSessionHelper()
        private val sparkSession: SparkSession by lazy { sparkSessionHelper.open() }

        @JvmStatic
        @AfterAll
        fun after(): Unit {
            sparkSessionHelper.close()
        }
    }
}