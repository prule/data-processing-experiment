package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class DescribeTest {

    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should describe`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(-1, 2)),
            GenericRow(arrayOf(10, 20)),
            GenericRow(arrayOf(5, 7)),
            GenericRow(arrayOf(null, 9)),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair("val1", DataTypes.IntegerType),
                Pair("val2", DataTypes.IntegerType),
            )
        )
        val statistic = Describe(listOf())
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 10
        result[0] shouldBe StatisticItem("describe", "val1", "count", "4")
        result[1] shouldBe StatisticItem("describe", "val1", "mean", "3.5")
        result[2] shouldBe StatisticItem("describe", "val1", "stddev", "5.066228051190222")
        result[3] shouldBe StatisticItem("describe", "val1", "min", "-1")
        result[4] shouldBe StatisticItem("describe", "val1", "max", "10")

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