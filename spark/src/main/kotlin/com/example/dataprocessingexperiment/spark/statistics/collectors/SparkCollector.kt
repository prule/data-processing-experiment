package com.example.dataprocessingexperiment.spark.statistics.collectors

import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

/**
 * Collects rows in memory and will write them out to CSV when close() is called.
 *
 * @param sparkSession spark session to use to create data frame which will be written out
 * @param path path to write dataframe to
 */
class SparkCollector(private val sparkSession: SparkSession, private val path: String) : Collector {

    private val data = mutableListOf<Row>()

    /**
     * Add the result to memory list for writing later (when close is called)
     */
    override fun add(key: String, column: String, discriminator: String, value: Any) {
        data.add(RowFactory.create(key, column, discriminator, value.toString()))
    }

    /**
     * Writes the collected data out to CSV.
     */
    override fun close() {

        val struct = StructType()
            .add("key", DataTypes.StringType, false)
            .add("column", DataTypes.StringType, true)
            .add("discriminator", DataTypes.StringType, true)
            .add("value", DataTypes.StringType, false)

        val dataFrame = sparkSession.createDataFrame(data, struct)

        dataFrame
            .write()
            .option("header", true)
            .format("csv")
            .save(path)

    }

}
