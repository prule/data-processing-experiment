package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.tables.Sources
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test


/**
https://github.com/Kotlin/kotlin-spark-api/blob/release/examples/src/main/kotlin/org/jetbrains/kotlinx/spark/examples/UDFs.kt
 */
class SparkUDFTest {

    private val dataHelper = SparkDataHelper(sparkSession)

    fun setupContext(): SparkContext {
        val data1 = listOf(
            GenericRow(arrayOf(-1, "a*")),
            GenericRow(arrayOf(10, "b")),
            GenericRow(arrayOf(5, "c*")),
            GenericRow(arrayOf(null, "d")),
        )

        val dataframe1 = dataHelper.asDataFrame(
            data1, listOf(
                Pair("val1", DataTypes.IntegerType),
                Pair("val2", DataTypes.StringType),
            )
        )

        val context = SparkContext(Sources("test", "test", "test", listOf()), sparkSession)
        context.set("dataFrame1", dataframe1)
        return context
    }


    var upper: UDF1<*, *> = UDF1<String, String> { s -> s.uppercase() }


    @Test
    fun `spark udf should work`() {

        // setup
        val context = setupContext()

        val df = context.get("dataFrame1")
        sparkSession.udf().register("mode", upper, DataTypes.StringType);
        df.select(col("val2")).show();
        df.select(callUDF("mode", col("val2"))).show();

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