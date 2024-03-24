package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.data.Types
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

class EmptyCountTest {
    private val columnName = "value"
    private val dataHelper = SparkDataHelper(sparkSession, true)

    @Test
    fun `should report nulls for integer`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(-1)),
            GenericRow(arrayOf(null)),
            GenericRow(arrayOf(10)),
            GenericRow(arrayOf(null)),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.IntegerType),
            )
        )
        val statistic = EmptyCount(listOf())
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBe StatisticItem("EmptyCount", "value", "", 2L)

    }

    @Test
    fun `should report NANs and NULLs for doubles`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf(-1.0)),
            GenericRow(arrayOf(Double.NaN)),
            GenericRow(arrayOf(10.0)),
            GenericRow(arrayOf(null)),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.DoubleType),
            )
        )
        val statistic = EmptyCount(listOf())
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBe StatisticItem("EmptyCount", "value", "", 2L)

    }

    @Test
    fun `should report Empty and NULLs for strings`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("A")),
            GenericRow(arrayOf(" ")),
            GenericRow(arrayOf(" NULL ")),
            GenericRow(arrayOf(null)),
        )
        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )
        dataframe.show()

        dataframe.select(Types.STRING.nullPredicate.invoke(col(columnName), false)).show()

        val statistic = EmptyCount(listOf())
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        dataframe.show()
        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBe StatisticItem("EmptyCount", "value", "", 3L)

    }

    @Test
    fun `should report Empty for strings`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("A")),
            GenericRow(arrayOf(" ")),
            GenericRow(arrayOf(" NULL ")),
            //  GenericRow(arrayOf(null)), // <-- Don't add a null when the schema doesn't allow nulls otherwise this error occurs
            // org.apache.spark.SparkException: [INTERNAL_ERROR] The Spark SQL phase optimization failed with an internal error. You hit a bug in Spark or the Spark plugins you use. Please, report this bug to the corresponding communities or vendors, and provide the full stack trace.
        )
        val dataframe = SparkDataHelper(sparkSession, false).asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )
        dataframe.show()

        val statistic = EmptyCount(listOf())
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        dataframe.show()
        // verify
        val result = collector.values()
        result.size shouldBeExactly 1
        result[0] shouldBe StatisticItem("EmptyCount", "value", "", 2L)

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