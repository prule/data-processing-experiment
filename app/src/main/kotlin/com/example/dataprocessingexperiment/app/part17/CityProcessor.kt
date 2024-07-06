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
class CityProcessor(
    override val id: String,
    override val name: String,
    override val description: String,
    val factTable: String,
    val sourceDimension: String,
    val targetDimension: String
) : Processor {

    override fun process(context: SparkContext) {
        val dataSet = context.get(factTable)
        val updatedDataset = dataSet.withColumn(
            targetDimension,
            `when`(upper(col(sourceDimension)).contains("\nHULL,"), "HULL")
                .otherwise(col(targetDimension))
        )

        context.set(factTable, updatedDataset)


    }

}