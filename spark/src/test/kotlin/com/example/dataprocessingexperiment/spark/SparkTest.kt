package com.example.dataprocessingexperiment.spark

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test

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