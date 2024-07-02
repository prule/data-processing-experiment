package com.example.dataprocessingexperiment.app.part17

import com.example.dataprocessingexperiment.spark.SparkContext
import com.example.dataprocessingexperiment.spark.pipeline.Processor
import kotlinx.serialization.Serializable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes

/**
 * // documentation for this class
 *
 */
@Serializable
class ValueContainsOneInListProcessor (
    override val id: String,
    override val name: String,
    override val description: String,
    val factTable: String,
    val factCol: String,
    val dimensionTable: String,
    val dimensionCol: String,
    val other: String
) : Processor {

    val udfName = "calculateValue"

    override fun process(context: SparkContext) {

        val dimensions = context.get(dimensionTable)
        val facts = context.get(factTable)
        val dimensionValues = dimensions.select(upper(col(dimensionCol))).collectAsList().map { row -> row.get(0).toString() }

        ValueCalculatorUDF(dimensionValues, other, udfName).registerUDF(context.sparkSession)

        val result = facts.withColumn(dimensionCol, callUDF(udfName, upper(col(factCol))))

        context.set(factTable, result)
    }


    private class ValueCalculatorUDF(names: List<String>, other: String, val udfName: String) {

        fun registerUDF(spark: SparkSession) {
            spark.udf().register(udfName, calculateValue, DataTypes.StringType);
        }

        var calculateValue: UDF1<*, *> = UDF1<String, String> { value ->
            for (name in names) {
                if (value.contains("\n$name,")) {
                    return@UDF1 name
                }
            }
            return@UDF1 other
        }

    }
}