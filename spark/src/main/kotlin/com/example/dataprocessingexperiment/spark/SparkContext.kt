package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.tables.Sources
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Stores datasets which can be accessed by processors.
 */
class SparkContext(
    val sources: Sources,
    val sparkSession: SparkSession,
    val datasetOutput: DatasetOutput = DatasetOutput()
) {
    private val data = mutableMapOf<String, Dataset<Row>>()

    /**
     * Add or update a dataset in the context
     */
    fun set(id: String, dataset: Dataset<Row>) {
        data.put(id, dataset)
    }

    /**
     * Get a dataset from the context
     */
    fun get(id: String): Dataset<Row> {
        if (contains(id)) {
            return data[id]!!
        } else {
            throw RuntimeException("Could not find dataset with id $id - context contains ${data.keys}")
        }
    }

    fun contains(id: String): Boolean {
        return data.containsKey(id)
    }

    fun tablesIds(): List<String> {
        return data.keys.toList()
    }

    fun show() {
        println("==============================================")
        println("Context")
        tablesIds().forEach {
            val dataset = get(it)
            // order by first column
            val firstColumn = dataset.columns().first()
            println("$it ordered by $firstColumn")
            datasetOutput.show(dataset.orderBy(firstColumn))
        }
        println("==============================================")
    }
}