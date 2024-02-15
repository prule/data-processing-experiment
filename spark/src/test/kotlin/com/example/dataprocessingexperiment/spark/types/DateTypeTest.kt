package com.example.dataprocessingexperiment.spark.types

import io.kotest.core.spec.style.AnnotationSpec
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.jetbrains.kotlinx.spark.api.*
import org.jetbrains.kotlinx.spark.api.tuples.tupleOf
import scala.collection.immutable.Seq
import java.sql.Date
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset


// https://github.com/Kotlin/kotlin-spark-api/blob/release/examples/src/main/kotlin/org/jetbrains/kotlinx/spark/examples/UdtRegistration.kt
class DateTypeTest : ShouldSpec({

    var sparkSession: SparkSession? = null

    beforeAny() {
        // spark setup
        println("session")
        val config = SparkConf().setAppName("spike").setMaster("local")
        sparkSession = SparkSession.builder().config(config).orCreate
    }

    should("convert valid dates") {
        val column = DateType().process("date", "date", listOf("yyyy-MM-dd"))
        val ds = sparkSession!!.dsOf("01-01-2000", "2020-01-01").toDF("date")

        val collectAsList = ds.select(column).map { it.getDate(0) }.collectAsList()
        collectAsList shouldContainExactlyInAnyOrder (listOf(
            null,
            Date(LocalDate.of(2020, 1, 1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli())
        ))
    }
})