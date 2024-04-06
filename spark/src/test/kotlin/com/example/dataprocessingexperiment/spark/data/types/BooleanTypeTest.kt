package com.example.dataprocessingexperiment.spark.data.types

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.SparkSessionHelper
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

// see the following link for examples
// https://github.com/Kotlin/kotlin-spark-api/blob/release/examples/src/main/kotlin/org/jetbrains/kotlinx/spark/examples/UdtRegistration.kt

class BooleanTypeTest {
    private val columnName = "value"
    private val dataHelper = SparkDataHelper(sparkSession)

    @Test
    fun `should convert valid booleans`() {

        // prepare

        val data = listOf(
            GenericRow(arrayOf("1")),
            GenericRow(arrayOf("true")),
            GenericRow(arrayOf("t")),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val column = BooleanType().process(columnName)

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

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair(columnName, DataTypes.StringType),
            )
        )

        val column = BooleanType().process(columnName)

        // perform
        val result = dataframe.select(column).collectAsList().map { it.get(0) }

        // verify
        result shouldContainExactlyInAnyOrder (listOf(
            null,
            null,
        ))
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