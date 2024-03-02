package com.example.dataprocessingexperiment.spark.statistics

import com.example.dataprocessingexperiment.Date
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItem
import com.example.dataprocessingexperiment.spark.statistics.collectors.StatisticItemCollector
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

class CountByMonthTest {
    private val columnName = "value"

    @Test
    fun `should calculate row count by month`() {

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

        val dataframe = asDataFrame(data)

        dataframe.show()

        val statistic = CountByMonth(columnName)
        val collector = StatisticItemCollector()

        // perform
        statistic.run(dataframe, collector)

        // verify
        val result = collector.values()
        result.size shouldBeExactly 4
        var i = 0
        result[i] shouldBeEqualToComparingFields StatisticItem("CountByMonth", "", 1)
        result[++i] shouldBeEqualToComparingFields StatisticItem("CountByMonth", "2020-01", 3)
        result[++i] shouldBeEqualToComparingFields StatisticItem("CountByMonth", "2020-02", 1)
        result[++i] shouldBeEqualToComparingFields StatisticItem("CountByMonth", "2020-03", 2)

    }

    private fun asDataFrame(data: List<GenericRow>): Dataset<Row> {
        return sparkSession.createDataFrame(
            data, StructType(
                arrayOf(
                    StructField(
                        // if we use nullable=false here, a null will be converted to "1970-01-01"
                        columnName, DataTypes.DateType, true, Metadata.empty(),
                    ),
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