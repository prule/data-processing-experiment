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

// see the following link for examples
// https://github.com/Kotlin/kotlin-spark-api/blob/release/examples/src/main/kotlin/org/jetbrains/kotlinx/spark/examples/UdtRegistration.kt

class DecimalTypeTest {
    private val columnName = "value"

    @Test
    fun `should convert valid numbers`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("1.12345")),
            GenericRow(arrayOf("1.12")),
        )

        val dataframe = asDataFrame(data)

        val column = DecimalType().process(columnName, listOf("10,3"))

        // perform
        val result = dataframe.select(column).collectAsList().map { it.getDecimal(0).stripTrailingZeros() }

        // verify
        result[0].compareTo(BigDecimal("1.123")) shouldBeExactly 0
        result[1].compareTo(BigDecimal("1.12")) shouldBeExactly 0
    }

    @Test
    fun `invalid numbers should be null`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("zzz")),
            GenericRow(arrayOf("1.12x")),
        )

        val dataframe = asDataFrame(data)

        val column = DecimalType().process(columnName, listOf("10,3"))

        // perform
        val result = dataframe.select(column).collectAsList().map { it.getDecimal(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            null,
            null
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