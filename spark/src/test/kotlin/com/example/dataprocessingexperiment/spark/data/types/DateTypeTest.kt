package com.example.dataprocessingexperiment.spark.data.types

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import mu.KotlinLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import java.sql.Date
import java.time.LocalDate
import java.time.ZoneId

// see the following link for examples
// https://github.com/Kotlin/kotlin-spark-api/blob/release/examples/src/main/kotlin/org/jetbrains/kotlinx/spark/examples/UdtRegistration.kt

class DateTypeTest {
    private val logger = KotlinLogging.logger {}
    private val columnName = "date"
    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should convert valid dates`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("01-01-2020")),
            GenericRow(arrayOf("2020-01-02")),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val column = DateType(listOf("yyyy-MM-dd", "dd-MM-yyyy")).process(columnName)

        // perform
        val result = dataframe.select(column).collectAsList().map { it.getDate(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            asDate(2020, 1, 1),
            asDate(2020, 1, 2)
        ))
    }

    @Test
    fun `invalid dates should be null`() {

        // prepare

        // these dates don't match the format we specify
        val data = listOf(
            GenericRow(arrayOf("100-01-2000")),
            GenericRow(arrayOf("01-01-2000")),
            GenericRow(arrayOf("Jan-2000")),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val column = DateType(listOf("yyyy-MM-dd")).process(columnName)

        // perform
        val result = dataframe.select(column).collectAsList().map { it.getDate(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            null,
            null,
            null,
        ))

    }

    private fun asDate(year: Int, month: Int, dayOfMonth: Int): Date {
        return Date(
            LocalDate.of(year, month, dayOfMonth).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
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