package com.example.dataprocessingexperiment.spark.data

import com.example.dataprocessingexperiment.spark.data.types.Types
import com.example.dataprocessingexperiment.tables.Column
import com.example.dataprocessingexperiment.tables.FileSource
import com.example.dataprocessingexperiment.tables.Table
import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.shouldBe
import mu.KotlinLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.Date
import java.time.LocalDate
import java.time.ZoneId

class DataFrameBuilderTypeTest {
    private val logger = KotlinLogging.logger {}

    @Test
    fun `data frame should build all the different types`() {

        logger.warn("Starting")
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // define our input source
        val fileSource = FileSource(
            "test1",
            "test csv file",
            "../data/sample1/types",
            "csv",
            // table structure
            Table(
                "test1",
                "test csv file",
                listOf(
                    Column("boolean", "boolean", "boolean"),
                    Column("date", "date", "date", listOf("d/M/yyyy", "yyyy-MM-dd")),
                    Column("decimal", "decimal", "decimal", listOf("10","2")),
                    Column("integer", "integer", "integer"),
                    Column("string", "string", "string"),
                    Column("unknown", "unknown", "unknown"), // test an invalid type definition is defaulted to string
                )
            ),
        )

        // build the dataframe
        val dataFrameBuilder = DataFrameBuilder(sparkSession, fileSource, Types.all())

        // typed dataset, only columns specified
        val typedDataset = dataFrameBuilder.typed()
        logger.debug {"Typed dataset"}
        typedDataset.printSchema()
        typedDataset.show(20)

        typedDataset.count() shouldBe 1

        val result = typedDataset.select("*").collectAsList()[0]

        result[0] shouldBe true
        result[1] shouldBeEqual Date(LocalDate.of(2020, 12, 31).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli())
        result[2] shouldBeEqual BigDecimal.valueOf(100.12)
        result[3] shouldBe 6
        result[4] shouldBe "any value"
        result[5] shouldBe "unknown"

        /*
            root
             |-- boolean: boolean (nullable = true)
             |-- date: date (nullable = true)
             |-- decimal: decimal(10,2) (nullable = true)
             |-- integer: integer (nullable = true)
             |-- string: string (nullable = true)

            +-------+----------+-------+-------+---------+
            |boolean|      date|decimal|integer|   string|
            +-------+----------+-------+-------+---------+
            |   true|2020-12-31| 100.12|      6|any value|
            +-------+----------+-------+-------+---------+
         */
    }

}