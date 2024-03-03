package com.example.dataprocessingexperiment.spark.data

import com.example.dataprocessingexperiment.spark.data.types.Types
import com.example.dataprocessingexperiment.tables.Column
import com.example.dataprocessingexperiment.tables.FileSource
import com.example.dataprocessingexperiment.tables.Table
import io.kotest.matchers.shouldBe
import mu.KotlinLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
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
        // code version of app/src/main/resources/sample1.statements.json5
        val fileSource = FileSource(
            "test1",
            "test1",
            "test csv file",
            "../data/sample1/transactions",
            "csv",
            // table structure
            Table(
                "test1",
                "test csv file",
                true,
                ",",
                listOf(
                    Column(listOf("date"), "date", "date", "date", listOf("yyyy-MM-dd", "dd-MM-yyyy"), required = true),
                    Column(listOf("account"), "account", "account", "string", required = true),
                    Column(listOf("description"), "description", "description", "string"),
                    Column(listOf("amount"), "amount", "amount", "decimal", listOf("10", "2"), required = true),
                )
            ),
        )

        val dataFrameBuilder = DataFrameBuilder(sparkSession, fileSource, Types.all())

        // raw dataset, no typing, all columns
        val rawDataset = dataFrameBuilder.raw
        display("raw", rawDataset)

        rawDataset.count() shouldBe 20

        // typed dataset, only columns specified
        val typedDataset = dataFrameBuilder.typed()
        display("typed", typedDataset)

        typedDataset.count() shouldBe 20

        // typed dataset, only columns specified
        val validDataset = dataFrameBuilder.valid(true)
        display("valid", validDataset)

        validDataset.count() shouldBe 12

    }

    fun display(name: String, ds: Dataset<Row>) {
        println()
        println(name)
        println()
        ds.printSchema()
        ds.orderBy("date").show(20)
        println("row count = ${ds.count()}")
    }

}