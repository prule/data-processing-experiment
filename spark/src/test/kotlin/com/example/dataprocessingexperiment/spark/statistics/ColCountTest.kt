package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.ints.shouldBeExactly
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class ColCountTest {
    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should calculate column count`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(-1, 0, 1, 2)),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair("col1", DataTypes.IntegerType),
                Pair("col2", DataTypes.IntegerType),
                Pair("col3", DataTypes.IntegerType),
                Pair("col4", DataTypes.IntegerType),
            )
        )
        val statistic = ColCount()
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBeEqualToComparingFields StatisticItem("column count", "", 4)

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