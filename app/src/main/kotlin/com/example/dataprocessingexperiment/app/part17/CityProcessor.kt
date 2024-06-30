package com.example.dataprocessingexperiment.app.part17

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.pipeline.Processor
import kotlinx.serialization.Serializable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes

/**
 * Either include or exclude rows that contain values in a specified column.
 * It has parameters for the table, column, values, and whether to exclude or include the values.
 */
@Serializable
class CityProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val addressTable: String,
    val cityTable: String,
    val column: String,
) : Processor {

    override fun process(context: SparkContext) {

        val cities = context.get(cityTable)
        val customers = context.get(addressTable)
        val cityNames = cities.select(upper(col(column))).collectAsList().map { row -> row.get(0).toString() }

        CityCalculatorUDF(cityNames).registerUDF(context.sparkSession)

        val result = customers.withColumn("city", callUDF("calculateCity", upper(col("address"))))

        context.set("customers", result)
    }


    private class CityCalculatorUDF(cityNames: List<String>) {

        fun registerUDF(spark: SparkSession) {
            spark.udf().register("calculateCity", calculateCity, DataTypes.StringType);
        }

        var calculateCity: UDF1<*, *> = UDF1<String, String> { value ->
            for (city in cityNames) {
                if (value.contains("\n$city,")) {
                    return@UDF1 city
                }
            }
            return@UDF1 "Other"
        }

    }
}