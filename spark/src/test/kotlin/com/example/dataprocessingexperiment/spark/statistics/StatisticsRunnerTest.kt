package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.equality.shouldBeEqualToComparingFields
import io.kotest.matchers.ints.shouldBeExactly
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class StatisticsRunnerTest {
    private val columnName = "value"

    @Test
    fun `should calculate statistics`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(-1)),
            GenericRow(arrayOf(10)),
            GenericRow(arrayOf(5)),
            GenericRow(arrayOf(null)),
        )

        val dataframe = asDataFrame(data)
        val collector = StatisticItemCollector()

        // perform
        StatisticsRunner().process(
            dataframe,
            listOf(
                Bounds(columnName),
                Count()
            ),
            collector
        )

        // verify
        val result = collector.values()
        result.size shouldBeExactly 3
        result shouldContain (StatisticItem("min", "", -1))
        result shouldContain (StatisticItem("max", "", 10))
        result shouldContain (StatisticItem("row count", "", 4L))
    }

    private fun asDataFrame(data: List<GenericRow>): Dataset<Row> {
        return sparkSession.createDataFrame(
            data, StructType(
                arrayOf(
                    StructField(
                        columnName, DataTypes.IntegerType, false, Metadata.empty()
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
        fun after() {
            sparkSessionHelper.close()
        }
    }
}