package com.example.dataprocessingexperiment.spark.data

import com.example.dataprocessingexperiment.spark.SparkDataHelper
import com.example.dataprocessingexperiment.spark.data.types.Types
import com.example.dataprocessingexperiment.spark.pipeline.JoinProcessorTest
import com.example.dataprocessingexperiment.tables.ColumnDefinition
import com.example.dataprocessingexperiment.tables.SourceDefinition
import com.example.dataprocessingexperiment.tables.TableDefinition
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import mu.KotlinLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DataTypes
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
        val sourceDefinition = SourceDefinition(
            "test1",
            "test1",
            "test csv file",
            "../data/sample1/transactions",
            "csv",
            // table structure
            TableDefinition(
                "test1",
                "test csv file",
                true,
                ",",
                listOf(
                    ColumnDefinition(
                        listOf("date"),
                        "date",
                        "date",
                        "date",
                        listOf("yyyy-MM-dd", "dd-MM-yyyy"),
                        required = true,
                    ),
                    ColumnDefinition(listOf("account"), "account", "account", "string", required = true),
                    ColumnDefinition(listOf("description"), "description", "description", "string"),
                    ColumnDefinition(
                        listOf("amount"),
                        "amount",
                        "amount",
                        "decimal",
                        listOf("10", "2"),
                        required = true
                    ),
                ),
                trim = true,
            ),
        )

        val dataFrameBuilder = DataFrameBuilder(sparkSession, sourceDefinition, Types.all())

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

    @Test
    fun `selected data frame should be trimmed`() {

        logger.warn("Starting")
        // spark setup
        val config = SparkConf().setAppName("spike").setMaster("local")
        val sparkSession = SparkSession.builder().config(config).orCreate
        val dataHelper = SparkDataHelper(sparkSession, true)

        // values with whitespace to be trimmed
        // all values should be the same when trimmed
        val data1 = listOf(
            GenericRow(arrayOf("a", "b")),
            GenericRow(arrayOf(" a ", "b ")),
            GenericRow(arrayOf("a ", " b ")),
            GenericRow(arrayOf("a ", " b")),
            GenericRow(arrayOf(null, null)),
        )

        val path = "./build/tmp/DataFrameBuilderTest"
        dataHelper.asDataFrame(
            data1, listOf(
                Pair("val1", DataTypes.StringType),
                Pair("val2", DataTypes.StringType),
            )
        ).write()
            .options(
                mapOf(
                    "header" to "true",
                    "ignoreLeadingWhiteSpace" to "false",
                    "ignoreTrailingWhiteSpace" to "false"
                )
            )
            .format("csv")
            .mode("overwrite")
            .save(path)

        // define our input source
        // code version of app/src/main/resources/sample1.statements.json5
        val sourceDefinition = SourceDefinition(
            "test1",
            "test1",
            "test csv file",
            path,
            "csv",
            // table structure
            TableDefinition(
                "test1",
                "test csv file",
                true,
                ",",
                listOf(
                    ColumnDefinition(listOf("val1"), "val1", "val1", "string", trim = true),
                    ColumnDefinition(listOf("val2"), "val2", "val2", "string", trim = true),
                )
            ),
        )

        val dataFrameBuilder = DataFrameBuilder(sparkSession, sourceDefinition, Types.all())

        // check we have some whitespace
        val raw = dataFrameBuilder.raw
        display("raw", raw)
        raw.collectAsList()
            .filter { hasLeadingOrTrailingWhitespace(it.getString(0)) || hasLeadingOrTrailingWhitespace(it.getString(1)) }.size shouldBeGreaterThan 0

        // selected dataset, only columns specified, trimmed as per column properties
        val selected = dataFrameBuilder.selected()
        display("selected", selected)

        selected.select("val1").collectAsList().filter { it.get(0) != null }
            .toSet().size shouldBe 1 // all values should be the same ("a") when trimmed
        selected.select("val2").collectAsList().filter { it.get(0) != null }
            .toSet().size shouldBe 1 // all values should be the same ("b") when trimmed

    }

    private fun hasLeadingOrTrailingWhitespace(str: String?) : Boolean {
        return str?.trim() != str
    }

    fun display(name: String, ds: Dataset<Row>) {
        println()
        println(name)
        println()
        ds.printSchema()
        if (ds.columns().contains("date")) {
            ds.orderBy("date").show(20)
        } else {
            ds.show(20)
        }
        println("row count = ${ds.count()}")
    }

}