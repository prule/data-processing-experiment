package com.example.dataprocessingexperiment.spark.data

import com.example.dataprocessingexperiment.spark.data.types.*
import com.example.dataprocessingexperiment.tables.ColumnDefinition
import com.example.dataprocessingexperiment.tables.DefaultCsvSourceType
import com.example.dataprocessingexperiment.tables.SourceDefinition
import com.example.dataprocessingexperiment.tables.TableDefinition
import io.kotest.data.headers
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
        val sourceDefinition = SourceDefinition(
            "test1",
            "test1",
            "test csv file",
            "../data/sample1/types",
            DefaultCsvSourceType().get(),
            // table structure
            TableDefinition(
                "test1",
                "test csv file",
                false,
                ",",
                listOf(
                    ColumnDefinition(listOf("boolean"), "boolean", "boolean", false, type = BooleanType()),
                    ColumnDefinition(listOf("date"), "date", "date", false, type = DateType(listOf("d/M/yyyy", "yyyy-MM-dd"))),
                    ColumnDefinition(listOf("decimal"), "decimal", "decimal", false, type = DecimalType(10,2)),
                    ColumnDefinition(listOf("integer"), "integer", "integer", false, type = IntegerType()),
                    ColumnDefinition(listOf("string"), "string", "string", false, type = StringType()),
                )
            ),
        )

        // build the dataframe
        val dataFrameBuilder = DataFrameBuilder(sparkSession, sourceDefinition)

        // typed dataset, only columns specified
        val typedDataset = dataFrameBuilder.typed()
        logger.debug { "Typed dataset" }
        typedDataset.printSchema()
        typedDataset.show(20)

        typedDataset.count() shouldBe 1

        val result = typedDataset.select("*").collectAsList()[0]

        result[0] shouldBe true
        result[1] shouldBeEqual Date(
            LocalDate.of(2020, 12, 31).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
        )
        result[2] shouldBeEqual BigDecimal.valueOf(100.12)
        result[3] shouldBe 6
        result[4] shouldBe "any value"

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


    @Test
    fun `data frame should build without headers`() {

        logger.warn("Starting")
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate

        // define our input source
        val sourceDefinition = SourceDefinition(
            "test1",
            "test1",
            "test csv file",
            "../data/sample1/types",
            DefaultCsvSourceType(header = false).get(),
            // table structure
            TableDefinition(
                "test1",
                "test csv file",
                false,
                ",",
                listOf(
                    ColumnDefinition(listOf("_c0"), "boolean", "boolean", false, type = BooleanType()),
                    ColumnDefinition(listOf("_c1"), "date", "date", false, type = DateType(listOf("d/M/yyyy", "yyyy-MM-dd"))),
                    ColumnDefinition(listOf("_c2"), "decimal", "decimal", false, type = DecimalType(10,2)),
                    ColumnDefinition(listOf("_c3"), "integer", "integer", false, type = IntegerType()),
                    ColumnDefinition(listOf("_c4"), "string", "string", false, type = StringType()),
                )
            ),
        )

        // build the dataframe
        val dataFrameBuilder = DataFrameBuilder(sparkSession, sourceDefinition)
        dataFrameBuilder.raw().show()
        dataFrameBuilder.selected().show()
    }

}