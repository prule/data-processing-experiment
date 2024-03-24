package com.example.dataprocessingexperiment.spark.data

import io.kotest.matchers.shouldBe
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class SparkDataTypesTest {

    @Test
    fun `should be numeric`() {
        val sparkDataTypes = SparkDataTypes()
        val result = sparkDataTypes.type(DataTypes.IntegerType)
        result shouldBe Types.NUMERIC
    }

    @Test
    fun `should be string`() {
        val sparkDataTypes = SparkDataTypes()
        val result = sparkDataTypes.type(DataTypes.StringType)
        result shouldBe Types.STRING
    }

    @Test
    fun `should be other`() {
        val sparkDataTypes = SparkDataTypes()
        val result = sparkDataTypes.type(DataTypes.BinaryType)
        result shouldBe Types.OTHER
    }
}