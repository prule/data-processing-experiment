package com.example.dataprocessingexperiment.spark

import org.apache.spark.sql.types.StructField
import scala.collection.immutable.List

class ScalaToKotlin {
    companion object {
        fun <T> toList(scalaList: List<T>): kotlin.collections.List<T> {
            val temp : MutableList<T> = mutableListOf()
            scalaList.foreach {
                temp.add(it)
            }
            return temp.toList()
        }
    }
}