package com.example.dataprocessingexperiment.spark.types

import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.ints.shouldBeExactly
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.Date
import java.time.LocalDate
import java.time.ZoneId

class IntegerTypeTest {
    private val columnName = "value"

    @Test
    fun `should convert valid numbers`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("1")),
            GenericRow(arrayOf("2")),
            GenericRow(arrayOf("3.1")),
            GenericRow(arrayOf("4.6")),
        )

        val dataframe = asDataFrame(data)

        val column = IntegerType().process(columnName, null)

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

        val dataframe = asDataFrame(data)

        val column = DecimalType().process(columnName, null)

        // perform
        val result = dataframe.select(column).collectAsList().map { it.get(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            null,
        ))
    }

    private fun asDataFrame(data: List<GenericRow>): Dataset<Row> {
        return sparkSession.createDataFrame(
            data, StructType(
                arrayOf(
                    StructField(
                        columnName, DataTypes.StringType, false, Metadata.empty()
                    )
                )
            )
        )
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