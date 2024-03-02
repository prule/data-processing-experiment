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

class BooleanTypeTest {
    private val columnName = "value"

    @Test
    fun `should convert valid booleans`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("1")),
            GenericRow(arrayOf("true")),
            GenericRow(arrayOf("t")),
        )

        val dataframe = asDataFrame(data)

        val column = BooleanType().process(columnName, listOf())

        // perform
        val result = dataframe.select(column).collectAsList().map { it.get(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            true,
            true,
            true,
        ))
    }

    @Test
    fun `should convert invalid booleans to false`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("")),
            GenericRow(arrayOf("x")),
        )

        val dataframe = asDataFrame(data)

        val column = BooleanType().process(columnName, listOf())

        // perform
        val result = dataframe.select(column).collectAsList().map { it.get(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            null,
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