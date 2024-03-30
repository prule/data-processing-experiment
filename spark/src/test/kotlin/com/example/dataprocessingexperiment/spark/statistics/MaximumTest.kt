package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class MaximumTest {
    private val columnName = "value"
    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should calculate bounds`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(-1)),
            GenericRow(arrayOf(10)),
            GenericRow(arrayOf(5)),
            GenericRow(arrayOf(null)),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.IntegerType),
            )
        )

        val statistic = Maximum("value")
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)


        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBe StatisticItem("max", columnName, "", 10)

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