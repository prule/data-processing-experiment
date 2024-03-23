package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.tables.Tables
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class SparkContext(val tables: Tables) {
    private val data = mutableMapOf<String, Dataset<Row>>()

    fun set(id: String, dataset: Dataset<Row>) {
        data.put(id, dataset)
    }

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
            println(it)
            get(it).show(100)
        }
        println("==============================================")
    }
}