package com.example.dataprocessingexperiment.spark

import io.kotest.matchers.shouldBe
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import scala.Tuple2

data class Item(
    val id: Int,
    val name: String
)

/*
 without setters on Item2 class I get the following exception (reproduce by commenting out the setters)

java.util.NoSuchElementException: None.get
	at scala.None$.get(Option.scala:627)
	at scala.None$.get(Option.scala:626)
	at org.apache.spark.sql.catalyst.DeserializerBuildHelper$.$anonfun$createDeserializer$8(DeserializerBuildHelper.scala:404)
	at scala.collection.immutable.ArraySeq.map(ArraySeq.scala:75)
	at scala.collection.immutable.ArraySeq.map(ArraySeq.scala:35)
	at org.apache.spark.sql.catalyst.DeserializerBuildHelper$.createDeserializer(DeserializerBuildHelper.scala:393)
	at org.apache.spark.sql.catalyst.DeserializerBuildHelper$.createDeserializer(DeserializerBuildHelper.scala:228)
	at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$.apply(ExpressionEncoder.scala:57)
	at org.apache.spark.sql.catalyst.encoders.ExpressionEncoder$.javaBean(ExpressionEncoder.scala:69)
	at org.apache.spark.sql.Encoders$.bean(Encoders.scala:179)
	at org.apache.spark.sql.Encoders.bean(Encoders.scala)
	at com.example.dataprocessingexperiment.spark.SparkTest.should create dataset from class(SparkTest.kt:101)
	at java.base/java.lang.reflect.Method.invoke(Method.java:568)
	at java.base/java.util.ArrayList.forEach(ArrayList.java:1511)
	at java.base/java.util.ArrayList.forEach(ArrayList.java:1511)

 */
class Item2(
    private var id: Int,
    private var name: String
) {

    fun getId(): Int {
        return id
    }

    fun setId(id: Int) {
        this.id = id
    }

    fun getName(): String {
        return name
    }

    fun setName(name: String) {
        this.name = name
    }
}


class SparkTest {

    @Test
    fun `replace values`() {
        val dataHelper = SparkDataHelper(sparkSession, true)

        val data = listOf(
            GenericRow(arrayOf(-1, "A")),
            GenericRow(arrayOf(10, null)),
            GenericRow(arrayOf(5, "B")),
            GenericRow(arrayOf(null, "c")),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair("col1", DataTypes.IntegerType),
                Pair("col2", DataTypes.StringType),
            )
        )

        dataframe.show()

        dataframe.withColumn(
            "col3",
            `when`(col("col2").equalTo("A"), "AA")
                .`when`(col("col2").equalTo("B"), "BB")
                .otherwise(
                    col("col2")
                )
        ).show()


    }

    @Test
    fun `count nulls`() {
        val dataHelper = SparkDataHelper(sparkSession, true)

        val data = listOf(
            GenericRow(arrayOf(-1, "A")),
            GenericRow(arrayOf(10, null)),
            GenericRow(arrayOf(5, "B")),
            GenericRow(arrayOf(null, null)),
        )

        val dataframe = dataHelper.asDataFrame(
            data, listOf(
                Pair("col1", DataTypes.IntegerType),
                Pair("col2", DataTypes.StringType),
            )
        )

        dataframe.show()

        dataframe.select(functions.count(col("col1").isNull)).show()

        dataframe.select(
            functions.count_if(col("col1").isNull).alias("col1"),
            functions.count_if(col("col2").isNull).alias("col2")
        ).show()

    }

    @Test
    fun `should list types`() {
        val data = listOf(
            Item(1, "a"),
            Item(2, "b")
        )

        val dataframe = sparkSession.createDataFrame(
            data,
            Item::class.java
        )

        val dtypes = dataframe.dtypes()

        dtypes[0] shouldBe Tuple2("id", "IntegerType")
        dtypes[1] shouldBe Tuple2("name", "StringType")

        val schema = dataframe.schema().toList()
        schema.foreach {
            if (it.dataType() is NumericType) {
                println("${it.name()} is numeric")
            }
            if (it.dataType() is BooleanType) {
                println("${it.name()} is boolean")
            }
            if (it.dataType() is StringType) {
                println("${it.name()} is binary")
            }

        }

    }

    @Test
    fun `should create dataframe from class`() {
        val data = listOf(
            Item(1, "a"),
            Item(2, "b")
        )

        val dataframe = sparkSession.createDataFrame(
            data,
            Item::class.java
        )

        dataframe.printSchema()
        dataframe.show()

        /*
            root
             |-- id: integer (nullable = false)
             |-- name: string (nullable = true)

            +---+----+
            | id|name|
            +---+----+
            |  1|   a|
            |  2|   b|
            +---+----+
         */
    }

    @Test
    fun `should create dataset from class`() {

        val data = listOf(
            Item2(1, "a"),
            Item2(2, "b")
        )

        val dataset = sparkSession.createDataset(
            data.toMutableList(),
            Encoders.bean(Item2::class.java)
        )

        dataset.printSchema()
        dataset.show()

        /*
            root
             |-- id: integer (nullable = false)
             |-- name: string (nullable = true)

            +---+----+
            | id|name|
            +---+----+
            |  1|   a|
            |  2|   b|
            +---+----+
         */
    }

    @Test
    fun `should create dataframe from schema`() {
        val data = listOf(
            GenericRow(arrayOf(1, "a")),
            GenericRow(arrayOf(2, "b")),
        )

        val dataframe = sparkSession.createDataFrame(
            data,
            StructType(
                arrayOf(
                    StructField(
                        "id",
                        DataTypes.IntegerType,
                        false,
                        org.apache.spark.sql.types.Metadata.empty()
                    ),
                    StructField(
                        "name",
                        DataTypes.StringType,
                        false,
                        org.apache.spark.sql.types.Metadata.empty()
                    )
                )
            )
        )

        dataframe.printSchema()
        dataframe.show()

        /*
            root
             |-- id: integer (nullable = false)
             |-- name: string (nullable = false)

            +---+----+
            | id|name|
            +---+----+
            |  1|   a|
            |  2|   b|
            +---+----+
         */
    }

    companion object {
        private val sparkSessionHelper = SparkSessionHelper()
        private val sparkSession: SparkSession by lazy { sparkSessionHelper.open() }

        @JvmStatic
        @AfterAll
        fun after(): Unit {
            sparkSessionHelper.close()
        }
    }
}