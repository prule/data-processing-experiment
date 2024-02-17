package com.example.dataprocessingexperiment.spark

import com.example.dataprocessingexperiment.spark.types.*
import com.example.dataprocessingexperiment.tables.Column
import com.example.dataprocessingexperiment.tables.FileSource
import com.example.dataprocessingexperiment.tables.Table
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.longs.shouldBeGreaterThan
import mu.KotlinLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class DataFrameBuilderTest {
    private val logger = KotlinLogging.logger {}

    @Test
    fun `data frame should build from code`() {

        logger.warn("Starting")
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // define our input source
        val fileSource = FileSource(
            "test1",
            "test csv file",
            "../data/sample1/statements",
            "csv",
            // table structure
            Table(
                "test1",
                "test csv file",
                listOf(
                    Column("account", "account", "string"),
                    Column("amount", "amount", "decimal", listOf("10","2")),
                    Column("date", "date", "date", listOf("d/M/yyyy", "yyyy-MM-dd")),
                    Column("description", "description", "string"),
                )
            ),
        )

        // register type converters
        val types = Types()
        types.add(DateType()) // this supports multiple configurable date formats
        types.add(DecimalType())

        //
        val dataFrameBuilder = DataFrameBuilder(sparkSession, fileSource, types)

        // raw dataset, no typing, all columns
        val rawDataset = dataFrameBuilder.raw
        logger.debug {"Raw dataset"}

        rawDataset.printSchema()
        rawDataset.show(20)

        rawDataset.count() shouldBeGreaterThan 10

        // typed dataset, only columns specified
        val typedDataset = dataFrameBuilder.typed()
        logger.debug {"Typed dataset"}
        typedDataset.printSchema()
        typedDataset.show(20)

        typedDataset.count() shouldBeGreaterThan 10

    }

}