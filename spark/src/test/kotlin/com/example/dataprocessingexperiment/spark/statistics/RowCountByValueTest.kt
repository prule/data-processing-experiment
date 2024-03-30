package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.Date
import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class RowCountByValueTest {
    private val columnName = "value"
    private val dataHelper = SparkDataHelper(sparkSession, true)

    @Test
    fun `should calculate row count by value`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(Date(2020, 1, 1).toSqlDate())),
            GenericRow(arrayOf(Date(2020, 1, 1).toSqlDate())),
            GenericRow(arrayOf(Date(2020, 1, 1).toSqlDate())),
            GenericRow(arrayOf(Date(2020, 2, 1).toSqlDate())),
            GenericRow(arrayOf(Date(2020, 3, 1).toSqlDate())),
            GenericRow(arrayOf(Date(2020, 3, 1).toSqlDate())),
            GenericRow(arrayOf(null)),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.DateType),
            )
        )

        val statistic = CountByValue(columnName)
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 4
        var i=0
        result[i] shouldBe StatisticItem("CountByValue", columnName, "", 1L)
        result[++i] shouldBe StatisticItem("CountByValue", columnName, "2020-01-01", 3L)
        result[++i] shouldBe StatisticItem("CountByValue", columnName, "2020-02-01", 1L)
        result[++i] shouldBe StatisticItem("CountByValue", columnName, "2020-03-01", 2L)

    }

    @Test
    fun `should calculate row count by value and trim string values`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("x")),
            GenericRow(arrayOf(" x")),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val statistic = CountByValue(columnName)
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBe StatisticItem("CountByValue", columnName, "x", 2L)

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